"""pairwise-object-pool-system - ObjectPoolSystem.

A complete object pool implementation:

* ``get_object``       - reuses pooled objects, enforces a size limit and
                         resets object state on checkout.
* ``return_object``    - rejects foreign objects, de-duplicates returns and
                         disables objects before pooling them.
* ``manage_pool``      - prewarming, dynamic expansion and idle cleanup.
* ``update_lifecycle`` - reference counting, delayed destruction and
                         event notification.
* ``get_pool_stats``   - hit rate, peak usage, per-category stats and alerts.
"""

import time
from collections import defaultdict


class PooledObject:
    """Default poolable object with reset/enable/disable lifecycle hooks."""

    def __init__(self):
        self.active = False
        self.state = {}

    def reset(self):
        self.state = {}
        self.active = False

    def enable(self):
        self.active = True

    def disable(self):
        self.active = False


class ObjectPoolSystem:
    DEFAULT_MAX_SIZE = 128
    DEFAULT_IDLE_TIMEOUT = 60.0
    DEFAULT_ALERT_THRESHOLD = 0.9

    def __init__(self, factory=None, max_size=DEFAULT_MAX_SIZE,
                 idle_timeout=DEFAULT_IDLE_TIMEOUT, destroy_delay=0.0,
                 alert_threshold=DEFAULT_ALERT_THRESHOLD):
        self.state = {}
        self.factory = factory or PooledObject
        self.max_size = max_size
        self.idle_timeout = idle_timeout
        self.destroy_delay = destroy_delay
        self.alert_threshold = alert_threshold

        self._available = defaultdict(list)      # category -> [obj, ...]
        self._in_use = {}                        # id(obj) -> {"obj", "category", "refcount"}
        self._known = set()                      # id(obj) of pool-originated objects
        self._last_used = {}                     # id(obj) -> monotonic timestamp
        self._pending_destroy = []               # [(obj, category, destroy_at), ...]
        self._pending_ids = set()                # id(obj) already scheduled for destroy
        self._listeners = defaultdict(list)      # event -> [callback, ...]
        self._alerts = []

        self._stats = defaultdict(lambda: {
            "hits": 0, "misses": 0, "created": 0,
            "returned": 0, "destroyed": 0, "peak_in_use": 0,
        })
        self._peak_total_in_use = 0

    # ------------------------------------------------------------------ #
    # checkout / checkin
    # ------------------------------------------------------------------ #
    def get_object(self, category="default", factory=None):
        """Take an object from the pool.

        Reuses a pooled object when one is available, enforces the pool
        size limit when creating new ones, and resets object state so no
        data leaks from the previous user.
        """
        stats = self._stats[category]
        pool = self._available[category]

        if pool:
            obj = pool.pop()
            stats["hits"] += 1
        else:
            stats["misses"] += 1
            if self.max_size and self._total_objects() >= self.max_size:
                # Size limit: reclaim the longest-idle pooled object to
                # make room instead of growing memory without bound.
                if not self._reclaim_idle():
                    self._alerts.append(
                        "pool_exhausted: all %d objects in use (category=%s)"
                        % (self.max_size, category))
            obj = self._create(category, factory)

        self._reset_object(obj)
        self._set_active(obj, True)
        self._known.add(id(obj))
        self._in_use[id(obj)] = {"obj": obj, "category": category, "refcount": 1}
        self._last_used[id(obj)] = time.monotonic()

        in_use = self._in_use_count(category)
        stats["peak_in_use"] = max(stats["peak_in_use"], in_use)
        self._peak_total_in_use = max(self._peak_total_in_use, len(self._in_use))
        self._notify("get", obj, category)
        return obj

    def return_object(self, obj=None, category=None):
        """Return an object to the pool.

        Only objects that originated from this pool and are currently
        checked out are accepted (foreign objects and duplicate returns
        are rejected). Accepted objects are disabled so they no longer
        render/update in the scene.
        """
        if obj is None:
            return {"returned": False, "reason": "no_object"}

        oid = id(obj)
        if oid not in self._known:
            return {"returned": False, "reason": "foreign_object"}
        if oid not in self._in_use:
            return {"returned": False, "reason": "duplicate_return"}

        entry = self._in_use[oid]
        entry["refcount"] -= 1
        if entry["refcount"] > 0:
            return {"returned": False, "reason": "still_referenced",
                    "refcount": entry["refcount"]}

        del self._in_use[oid]
        cat = category or entry["category"]
        self._set_active(obj, False)
        self._available[cat].append(obj)
        self._last_used[oid] = time.monotonic()
        self._stats[cat]["returned"] += 1
        self._notify("return", obj, cat)
        return {"returned": True, "object": obj, "category": cat}

    # ------------------------------------------------------------------ #
    # pool management
    # ------------------------------------------------------------------ #
    def manage_pool(self, prewarm=0, category="default", factory=None,
                    grow_by=0, idle_timeout=None):
        """Manage pool capacity.

        * ``prewarm``  - create objects up front so the first checkout
          does not stall.
        * ``grow_by``  - dynamically expand the pool (up to ``max_size``)
          when the category has no available objects.
        * idle cleanup - destroy available objects that have been unused
          longer than ``idle_timeout`` (defaults to the pool setting).
        """
        created = 0
        for _ in range(max(0, prewarm)):
            if self.max_size and self._total_objects() >= self.max_size:
                break
            self._available[category].append(self._create(category, factory))
            created += 1

        expanded = 0
        if grow_by > 0 and not self._available[category]:
            for _ in range(grow_by):
                if self.max_size and self._total_objects() >= self.max_size:
                    self._alerts.append(
                        "pool_at_capacity: cannot expand category=%s" % category)
                    break
                self._available[category].append(self._create(category, factory))
                expanded += 1

        cleaned = self._cleanup_idle(idle_timeout)
        return {
            "prewarmed": created,
            "expanded": expanded,
            "cleaned": cleaned,
            "size": self._total_objects(),
        }

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    def acquire(self, obj):
        """Increment the reference count of an in-use object."""
        entry = self._in_use.get(id(obj))
        if entry is not None:
            entry["refcount"] += 1
            return entry["refcount"]
        return 0

    def destroy(self, obj, delay=None):
        """Schedule an object for destruction.

        Destruction is delayed (default: the pool's ``destroy_delay``) so
        objects that are still playing animations are not removed
        immediately, and objects with outstanding references are not
        destroyed until their refcount reaches zero.
        """
        oid = id(obj)
        if oid not in self._known:
            return {"destroyed": False, "reason": "foreign_object"}
        if oid in self._pending_ids:
            return {"destroyed": False, "scheduled": False,
                    "reason": "already_scheduled"}
        if oid in self._in_use and self._in_use[oid]["refcount"] > 0:
            delay = self.destroy_delay if delay is None else delay
        else:
            delay = 0.0 if delay is None else delay
        for pooled in self._available.values():
            if obj in pooled:
                pooled.remove(obj)
        destroy_at = time.monotonic() + max(0.0, delay)
        self._pending_destroy.append((obj, self._category_of(obj), destroy_at))
        self._pending_ids.add(oid)
        return {"destroyed": False, "scheduled": True, "delay": max(0.0, delay)}

    def update_lifecycle(self, dt=0.0):
        """Advance object lifecycles.

        Processes pending destructions whose delay has elapsed. Objects
        that are still referenced (refcount > 0) are deferred instead of
        being recycled while in use. Every destruction fires the
        registered ``destroy`` event callbacks.
        """
        now = time.monotonic() + max(0.0, dt)
        destroyed = 0
        still_pending = []
        for obj, category, destroy_at in self._pending_destroy:
            if id(obj) not in self._known:
                self._pending_ids.discard(id(obj))
                continue
            entry = self._in_use.get(id(obj))
            if entry is not None and entry["refcount"] > 0:
                # Still in use: defer instead of recycling a live object.
                still_pending.append((obj, category, destroy_at))
                continue
            if now >= destroy_at:
                self._destroy(obj, category)
                self._pending_ids.discard(id(obj))
                destroyed += 1
            else:
                still_pending.append((obj, category, destroy_at))
        self._pending_destroy = still_pending
        return {
            "destroyed": destroyed,
            "pending": len(self._pending_destroy),
            "in_use": len(self._in_use),
        }

    def on(self, event, callback):
        """Register an event callback (get/return/destroy/alert)."""
        self._listeners[event].append(callback)
        return callback

    # ------------------------------------------------------------------ #
    # statistics
    # ------------------------------------------------------------------ #
    def get_pool_stats(self, category=None):
        """Return pool statistics.

        Tracks hit rate and peak usage (globally and per category) and
        raises alerts when the pool is close to its size limit.
        """
        per_category = {}
        for cat in set(self._stats) | set(self._available):
            s = self._stats[cat]
            requests = s["hits"] + s["misses"]
            per_category[cat] = {
                **s,
                "requests": requests,
                "hit_rate": s["hits"] / requests if requests else 0.0,
                "available": len(self._available[cat]),
                "in_use": self._in_use_count(cat),
            }
        if category is not None:
            return per_category.get(category, {
                "hits": 0, "misses": 0, "created": 0, "returned": 0,
                "destroyed": 0, "peak_in_use": 0, "requests": 0,
                "hit_rate": 0.0, "available": 0, "in_use": 0,
            })

        total_hits = sum(s["hits"] for s in self._stats.values())
        total_requests = total_hits + sum(s["misses"] for s in self._stats.values())
        size = self._total_objects()

        alerts = list(self._alerts)
        if self.max_size and size >= self.max_size * self.alert_threshold:
            alerts.append("pool_near_capacity: %d/%d objects allocated"
                          % (size, self.max_size))

        return {
            "total_objects": size,
            "available": sum(len(v) for v in self._available.values()),
            "in_use": len(self._in_use),
            "hit_rate": total_hits / total_requests if total_requests else 0.0,
            "peak_in_use": self._peak_total_in_use,
            "categories": per_category,
            "alerts": alerts,
        }

    # ------------------------------------------------------------------ #
    # internals
    # ------------------------------------------------------------------ #
    def _create(self, category, factory=None):
        maker = factory or self.factory
        obj = maker() if callable(maker) else maker
        self._known.add(id(obj))
        self._last_used[id(obj)] = time.monotonic()
        self._stats[category]["created"] += 1
        return obj

    def _destroy(self, obj, category):
        self._known.discard(id(obj))
        self._in_use.pop(id(obj), None)
        self._last_used.pop(id(obj), None)
        self._stats[category]["destroyed"] += 1
        self._notify("destroy", obj, category)

    def _reclaim_idle(self):
        oldest = None
        for cat, objs in self._available.items():
            for obj in objs:
                used_at = self._last_used.get(id(obj), 0.0)
                if oldest is None or used_at < oldest[0]:
                    oldest = (used_at, cat, obj)
        if oldest is None:
            return False
        _, cat, obj = oldest
        self._available[cat].remove(obj)
        self._destroy(obj, cat)
        return True

    def _cleanup_idle(self, idle_timeout=None):
        timeout = self.idle_timeout if idle_timeout is None else idle_timeout
        if timeout is None:
            return 0
        now = time.monotonic()
        cleaned = 0
        for cat in list(self._available):
            survivors = []
            for obj in self._available[cat]:
                if now - self._last_used.get(id(obj), now) >= timeout:
                    self._destroy(obj, cat)
                    cleaned += 1
                else:
                    survivors.append(obj)
            self._available[cat] = survivors
        return cleaned

    def _reset_object(self, obj):
        reset = getattr(obj, "reset", None)
        if callable(reset):
            reset()
        elif hasattr(obj, "__dict__"):
            obj.__dict__.clear()

    def _set_active(self, obj, active):
        hook = getattr(obj, "enable" if active else "disable", None)
        if callable(hook):
            hook()
        else:
            try:
                obj.active = active
            except (AttributeError, TypeError):
                pass

    def _category_of(self, obj):
        entry = self._in_use.get(id(obj))
        if entry is not None:
            return entry["category"]
        for cat, objs in self._available.items():
            if obj in objs:
                return cat
        return "default"

    def _total_objects(self):
        return len(self._known)

    def _in_use_count(self, category):
        return sum(1 for e in self._in_use.values() if e["category"] == category)

    def _notify(self, event, obj, category):
        for callback in self._listeners.get(event, ()):
            callback(obj, category)

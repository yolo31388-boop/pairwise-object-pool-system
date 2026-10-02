"""
pairwise-object-pool-system - ObjectPoolSystem

This module contains a deliberately broken implementation.
Fix all bugs so that tests pass.
"""


class ObjectPoolSystem:
    def __init__(self):
        self.state = {}

    def get_object(self, *args, **kwargs):
        """BUG: 取对象时不检查池里有没有，永远新建"""
        return None

    def return_object(self, *args, **kwargs):
        """BUG: 归还对象不检查是不是池里的，外部对象也能放进来"""
        return None

    def manage_pool(self, *args, **kwargs):
        """BUG: 池管理不做预热，第一次取对象才创建"""
        return None

    def update_lifecycle(self, *args, **kwargs):
        """BUG: 生命周期不做引用计数，对象还在用就被回收"""
        return None

    def get_pool_stats(self, *args, **kwargs):
        """BUG: 池统计只记录数量，不记录命中率和峰值"""
        return None


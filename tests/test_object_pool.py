import pytest
from object_pool.system import ObjectPoolSystem


class TestObjectPoolSystem:

    def test_get_object(self):
        """取对象时不检查池里有没有，永远新建"""
        obj = ObjectPoolSystem()
        result = obj.get_object()
        self.assertIsNotNone(result)

    def test_return_object(self):
        """归还对象不检查是不是池里的，外部对象也能放进来"""
        obj = ObjectPoolSystem()
        result = obj.return_object()
        self.assertIsNotNone(result)

    def test_manage_pool(self):
        """池管理不做预热，第一次取对象才创建"""
        obj = ObjectPoolSystem()
        result = obj.manage_pool()
        self.assertIsNotNone(result)

    def test_update_lifecycle(self):
        """生命周期不做引用计数，对象还在用就被回收"""
        obj = ObjectPoolSystem()
        result = obj.update_lifecycle()
        self.assertIsNotNone(result)

    def test_get_pool_stats(self):
        """池统计只记录数量，不记录命中率和峰值"""
        obj = ObjectPoolSystem()
        result = obj.get_pool_stats()
        self.assertIsNotNone(result)


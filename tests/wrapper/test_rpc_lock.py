import unittest

from dlvm.wrapper.rpc_lock import RpcLock, LockError


class RpcLockTest(unittest.TestCase):

    def setUp(self):
        self.rpc_lock = RpcLock()

    def test_res_lock(self):
        with self.rpc_lock.res_lock('foo'):
            with self.rpc_lock.res_lock('bar'):
                pass

    def test_global_lock(self):
        with self.rpc_lock.global_lock():
            pass

    def test_res_res_conflict(self):
        with self.rpc_lock.res_lock('foo'):
            with self.assertRaises(LockError):
                with self.rpc_lock.res_lock('foo'):
                    pass

    def test_res_global_conflict(self):
        with self.rpc_lock.res_lock('foo'):
            with self.assertRaises(LockError):
                with self.rpc_lock.global_lock():
                    pass

    def test_global_res_conflict(self):
        with self.rpc_lock.global_lock():
            with self.assertRaises(LockError):
                with self.rpc_lock.res_lock('foo'):
                    pass

    def test_global_global_conflict(self):
        with self.rpc_lock.global_lock():
            with self.assertRaises(LockError):
                with self.rpc_lock.global_lock():
                    pass

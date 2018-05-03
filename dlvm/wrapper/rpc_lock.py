from threading import Lock


class LockContext():

    def __init__(self, lock, res_set, global_lock):
        self.lock = lock
        self.res_set = res_set
        self.global_lock = global_lock

    def __str__(self):
        return '<{0} {1} {2}>'.format(
            self.lock, self.res_set, self.global_lock)


class LockError(Exception):

    def __init__(self, res_name, lock_ctx):
        self.message = '{0} {1}'.format(res_name, lock_ctx)
        super(LockError, self).__init__(self.message)


class ResLock():

    def __init__(self, res_name, lock_ctx):
        self.res_name = res_name
        self.lock_ctx = lock_ctx

    def __enter__(self):
        with self.lock_ctx.lock:
            if self.lock_ctx.global_lock is True:
                raise LockError(self.res_name, self.lock_ctx)
            if self.res_name in self.lock_ctx.res_set:
                raise LockError(
                    self.res_name, self.lock_ctx)
            self.lock_ctx.res_set.add(self.res_name)

    def __exit__(self, exc_type, exc_value, traceback):
        with self.lock_ctx.lock:
            self.lock_ctx.res_set.remove(self.res_name)


class GlobalLock():

    def __init__(self, lock_ctx):
        self.lock_ctx = lock_ctx

    def __enter__(self):
        with self.lock_ctx.lock:
            if self.lock_ctx.global_lock is True:
                raise LockError('global_lock', self.lock_ctx)
            if self.lock_ctx.res_set:
                raise LockError('global_lock', self.lock_ctx)
            self.lock_ctx.global_lock = True

    def __exit__(self, exc_type, exc_value, traceback):
        with self.lock_ctx.lock:
            self.lock_ctx.global_lock = False


class RpcLock():

    def __init__(self):
        self.lock_ctx = LockContext(Lock(), set(), False)

    def res_lock(self, res_name):
        return ResLock(res_name, self.lock_ctx)

    def global_lock(self):
        return GlobalLock(self.lock_ctx)

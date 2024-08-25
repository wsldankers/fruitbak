from threading import RLock

from fruitbak.util import (
    Initializer,
    ThreadPool,
    initializer,
    locked,
    stub,
    weakproperty,
)


class Handler(Initializer):
    def __init__(self, *args, **kwargs):
        self.lock = RLock()
        return super().__init__(*args, **kwargs)

    @weakproperty
    def fruitbak(self):
        return self.pool.fruitbak

    @weakproperty
    def pool(self):
        raise RuntimeError("%s.pool used uninitialized" % (type(self).__name__,))

    @initializer
    def config(self):
        return self.pool.config

    max_workers = 32

    @locked
    @initializer
    def executor(self):
        return ThreadPool(max_workers=self.max_workers)

    @initializer
    def cpu_executor(self):
        return self.fruitbak.cpu_executor

    @stub
    def has_chunk(self, callback, hash):
        pass

    @stub
    def get_chunk(self, callback, hash):
        pass

    @stub
    def put_chunk(self, callback, hash, value):
        pass

    @stub
    def del_chunk(self, callback, hash):
        pass

    @stub
    def lister(self, agent):
        pass


class Filter(Handler):
    def __init__(self, subordinate, **kwargs):
        super().__init__(**kwargs)
        self.subordinate = subordinate

    @weakproperty
    def pool(self):
        return self.subordinate.pool

    def has_chunk(self, callback, hash):
        return self.subordinate.has_chunk(callback, hash)

    def get_chunk(self, callback, hash):
        return self.subordinate.get_chunk(callback, hash)

    def put_chunk(self, callback, hash, value):
        return self.subordinate.put_chunk(callback, hash, value)

    def del_chunk(self, callback, hash):
        return self.subordinate.del_chunk(callback, hash)

    def lister(self, agent):
        return self.subordinate.lister(agent)

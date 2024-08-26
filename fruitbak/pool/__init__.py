"""Chunk-based deduplicating bulk data storage system for Fruitbak.

Fruitbak uses this subsystem to store file contents (but not file
metadata). File contents are split into chunks of (by default) 2MiB and
stored by their (by default) SHA256 hash.

The pool data can be stored on a local disk, over the network or even
mirrored or sharded over any combination. Filters can be inserted that
perform operations like compression, encryption or forward error
correction.

Pool operations are highly parallelized to ensure that high latency storage
mechanisms (such as remote networks) still have acceptable throughput.

To access the pool you should create a `PoolAgent` object that will act as
an intermediary. The pool system will ensure that all `PoolAgent` objects
get an equal share of the available I/O bandwidth.

Unless you are implementing a storage method or filter you should never
need anything from this module except the `Pool.agent()` method (which
creates a new agent)."""

from weakref import WeakValueDictionary

from fruitbak.config import configurable, configurable_function
from fruitbak.pool.agent import PoolAgent
from fruitbak.pool.storage import Filesystem
from fruitbak.util import (
    Initializer,
    MinWeakHeapMap,
    NLock,
    initializer,
    locked,
    weakproperty,
)


class Pool(Initializer):
    """Represents the pool subsystem and maintains the tree of storage
    implementation(s) and filters.

    You should not access this object directly except to create a `PoolAgent`
    object (see the `agent` method). You can use this `PoolAgent` to access all
    pool related functionality.

    You should never create a `Pool` object yourself but always use the
    `Fruitbak.pool` attribute."""

    def __init__(self, *args, **kwargs):
        self.lock = NLock()
        super().__init__(*args, **kwargs)

    queue_depth = 0

    @initializer
    def config(self):
        """The global Fruitbak configuration object.

        :type: fruitbak.Config"""

        return self.fruitbak.config

    @configurable('pool_max_queue_depth')
    def max_queue_depth(self):
        """The maximum number of operations that may be in progress at a given time
        for the pool to schedule more readahead operations.

        Note that this number may be exceeded if a sufficient number of agent
        objects have active readahead queues (agents are guaranteed to be able to
        perform at least one readahead operation, to prevent deadlocks).

        This property is user-configurable under the name `pool_max_queue_depth`.

        :type: int"""

        return 32

    @weakproperty
    def fruitbak(self):
        """A weak reference to the `Fruitbak` object that created us. For internal
        purposes.

        :type: fruitbak.Fruitbak"""

        raise RuntimeError("pool.fruitbak used uninitialized")

    @configurable
    def pool_storage_type(self):
        """The type of the default storage subsystem to use. Only used by
        `pool_storage`, and only if that is not user configured.

        This property is user-configurable.

        :type: type"""

        return Filesystem

    @configurable
    def pool_storage_options(self):
        """The options of the default storage subsystem to use. Only used by
        `pool_storage`, and only if that is not user configured.

        This property is user-configurable.

        :type: dict"""

        return {}

    @configurable_function
    def pool_storage(pool):
        """A function that instantiates the storage/filter tree.

        Uses `pool_storage_type` and `pool_storage_options` if not overridden by
        the user.

        This property is user-configurable.

        :type: function"""

        return pool.pool_storage_type(pool=pool, **pool.pool_storage_options)

    @configurable
    def pool_encryption_key(self):
        """The default encryption key that is used if encryption filters are in use
        and any of the encryption filters does not have an explicitly configured
        key.

        Defaults to `None`, which should cause the encryption filter to raise an
        error and suggest a value.

        If the key is a str it is assumed to be base64-encoded.

        This property is user-configurable.

        :type: str or bytes or None"""

        return None

    @initializer
    def root(self):
        """The root of the storage/filter tree. Initialized using `pool_storage`.

        For internal use only.

        The pool lock must be held while accessing this attribute.

        :type: fruitbak.pool.handler.Handler"""

        assert self.lock
        return self.pool_storage(self)

    @initializer
    def agents(self):
        """A weak heapmap containing all extant `PoolAgent` objects associated with
        this pool. This heapmap is ordered by the number of pending operations of
        each `PoolAgent` object.

        The pool lock must be held while accessing this attribute.

        :type: fruitbak.util.MinWeakHeapMap"""

        assert self.lock
        return MinWeakHeapMap()

    @initializer
    def chunk_registry(self):
        """A weak dictionary of known chunks, to ensure that chunks are kept in
        memory only once. Only used by `exchange_chunk()`.

        The pool lock must be held while accessing this attribute.

        :type: weakref.WeakValueDictionary"""

        assert self.lock
        return WeakValueDictionary()

    def exchange_chunk(self, hash, chunk=None):
        """Exchange the given chunk for an already existing copy, if such a copy
        exists. If it does not (and `chunk` is not `None`), the chunk is stored for
        future use.

        The pool lock must be held while calling this method.

        :param bytes hash: The hash of the requested chunk.
        :param chunk: A chunk that has the given hash.
        :type chunk: bytes or None
        :return: The already existing chunk if it exists, or `chunk` if not.
        :rtype: bytes or None"""

        assert self.lock
        # can't use setdefault(), it has weird corner cases
        # involving None
        chunk_registry = self.chunk_registry
        try:
            return chunk_registry[hash]
        except KeyError:
            pass
        if chunk is not None:
            try:
                chunk_registry[hash] = chunk
            except TypeError:
                # bytes objects can't be weakref'd
                chunk = memoryview(chunk)
                chunk_registry[hash] = chunk
        return chunk

    def agent(self, *args, **kwargs):
        """Create and return a `PoolAgent` object for this pool.
        Any arguments are passed to the constructor.

        :param \\*args: Passed to the `PoolAgent` constructor.
        :param \\*\\*kwargs: Passed to the `PoolAgent` constructor.
        :return: An agent object.
        :rtype: PoolAgent"""

        return PoolAgent(pool=self, *args, **kwargs)

    def register_agent(self, agent):
        """(Re-)register an agent with this pool. Intended for agents
        to register theirselves.

        Registered agents are eligible to schedule new requests when
        there are slots available in the queue.

        The pool lock must be held while calling this method.

        :param PoolAgent agent: The agent to register."""

        assert self.lock
        new = agent.avarice
        agents = self.agents
        try:
            old = agents[agent]
        except KeyError:
            pass
        else:
            if old == new:
                return
        self.agents[agent] = new

    def unregister_agent(self, agent):
        """Unregister an agent from this pool. Intended for agents
        to unregister theirselves.

        When an agent is unregistered it will no longer be called upon
        to schedule I/O operations.

        The pool lock must be held while calling this method.

        :param PoolAgent agent: The agent to unregister."""

        assert self.lock
        self.agents.discard(agent)

    def replenish_queue(self):
        """Allow agents to schedule I/O operations. Will loop for as long
        as it takes to either fill up the queue to `max_queue_depth`
        or until no agents have pending I/O.

        The pool lock must be held while calling this method."""

        assert self.lock
        agents = self.agents
        while agents and self.queue_depth < self.max_queue_depth:
            agent = agents.peekkey()
            if agent is None:
                break
            agent.dequeue()

    def has_chunk(self, callback, hash):
        """Submit a request to check the existence of a chunk in the pool.

        The callback is a function that will be called with the result once
        the operation has completed or an exception occurred.
        It is called with two arguments, the result value (a boolean) and any
        exception that occurred. Exactly one of these is always `None`.

        The pool lock must be held while calling this method.

        :param function callback: Called when the I/O completed (or failed).
        :param bytes hash: The hash of the chunk to check for."""

        return self.submit(self.root.has_chunk, callback, hash)

    def get_chunk(self, callback, hash):
        """Submit a request to fetch a chunk from the pool.

        The callback is a function that will be called with the result once
        the operation has completed or an exception occurred.
        It is called with two arguments, the result value (a bytes-like object) and
        any exception that occurred. Exactly one of these is always `None`.

        The pool lock must be held while calling this method.

        :param function callback: Called when the I/O completed (or failed).
        :param bytes hash: The hash of the chunk to fetch."""

        return self.submit(self.root.get_chunk, callback, hash)

    def put_chunk(self, callback, hash, value):
        """Submit a request to store a chunk in the pool.

        The callback is a function that will be called with the result once
        the operation has completed or an exception occurred.
        It is called with one argument which is the exception that occurred (if
        any). If the command completed succesfully it is `None`.

        The pool lock must be held while calling this method.

        :param function callback: Called when the I/O completed (or failed).
        :param bytes hash: The hash of the chunk to store.
        :param bytes value: The contents of the chunk to store."""

        return self.submit(self.root.put_chunk, callback, hash, value)

    def del_chunk(self, callback, hash):
        """Submit a request to delete a chunk from the pool.

        The callback is a function that will be called with the result once
        the operation has completed or an exception occurred.
        It is called with one argument which is the exception that occurred (if
        any). If the command completed succesfully it is `None`.

        The pool lock must be held while calling this method.

        :param function callback: Called when the I/O completed (or failed).
        :param bytes hash: The hash of the chunk to delete."""

        return self.submit(self.root.del_chunk, callback, hash)

    def submit(self, func, callback, *args, **kwargs):
        """Submit an I/O request. For internal use only.

        Submits the request (represented by the `func` argument) and takes care of
        updating the current queue depth counter.

        The pool lock must be held while calling this method.

        :param function func: The operation to queue.
        :param function callback: Called when the I/O completed (or failed).
        :param \\*args: Passed to `func`.
        :param \\*\\*kwargs: Passed to `func`."""

        lock = self.lock
        assert lock
        self.queue_depth += 1

        def when_done(*args, **kwargs):
            try:
                callback(*args, **kwargs)
            finally:
                with lock:
                    self.queue_depth -= 1
                    self.replenish_queue()

        return func(when_done, *args, **kwargs)

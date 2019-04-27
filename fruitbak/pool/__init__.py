from weakref import ref as weakref, WeakValueDictionary

from threading import Condition, RLock
from sys import stderr

from fruitbak.util import Initializer, initializer, MinWeakHeapMap, weakproperty, locked, NLock
from fruitbak.pool.storage import Filesystem
from fruitbak.pool.agent import PoolAgent
from fruitbak.config import configurable, configurable_function

class Pool(Initializer):
	def __init__(self, *args, **kwargs):
		self.lock = NLock()
		super().__init__(*args, **kwargs)

	queue_depth = 0

	@initializer
	def config(self):
		return self.fruitbak.config

	@configurable
	def max_queue_depth(self):
		return 32

	@weakproperty
	def fruitbak(self):
		raise RuntimeError("pool.fruitbak used uninitialized")

	@initializer
	def config(self):
		return self.fruitbak.config

	@configurable
	def pool_storage_type(self):
		return Filesystem

	@configurable
	def pool_storage_options(self):
		return {}

	@configurable_function
	def pool_storage(pool):
		return pool.pool_storage_type(pool = pool, **pool.pool_storage_options)

	@configurable
	def pool_encryption_key(self):
		return None

	@initializer
	def root(self):
		assert self.lock
		return self.pool_storage(self)

	@initializer
	def agents(self):
		assert self.lock
		return MinWeakHeapMap()

	@initializer
	def chunk_registry(self):
		assert self.lock
		return WeakValueDictionary()

	def exchange_chunk(self, hash, new_chunk = None):
		assert self.lock
		# can't use setdefault(), it has weird corner cases
		# involving None
		chunk_registry = self.chunk_registry
		old_chunk = chunk_registry.get(hash)
		if old_chunk is not None:
			return old_chunk
		if new_chunk is not None:
			try:
				chunk_registry[hash] = new_chunk
			except TypeError:
				new_chunk = memoryview(new_chunk)
				chunk_registry[hash] = new_chunk
		return new_chunk

	def agent(self, *args, **kwargs):
		return PoolAgent(pool = self, *args, **kwargs)

	def register_agent(self, agent):
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
		assert self.lock
		self.agents.discard(agent)

	def replenish_queue(self):
		assert self.lock
		agents = self.agents
		while agents and self.queue_depth < self.max_queue_depth:
			agent = agents.peekkey()
			if agent is None:
				break
			agent.dequeue()

	def has_chunk(self, callback, hash):
		return self.submit(self.root.has_chunk, callback, hash)

	def get_chunk(self, callback, hash):
		return self.submit(self.root.get_chunk, callback, hash)

	def put_chunk(self, callback, hash, value):
		return self.submit(self.root.put_chunk, callback, hash, value)

	def del_chunk(self, callback, hash):
		return self.submit(self.root.del_chunk, callback, hash)

	def submit(self, func, callback, *args, **kwargs):
		lock = self.lock
		assert lock
		#with lock:
		self.queue_depth += 1

		def when_done(*args, **kwargs):
			try:
				callback(*args, **kwargs)
			finally:
				with lock:
					self.queue_depth -= 1
					self.replenish_queue()
		return func(when_done, *args, **kwargs)

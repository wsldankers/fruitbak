from weakref import ref as weakref, WeakValueDictionary

from threading import Condition, RLock
from sys import stderr

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.weakheapmap import MinWeakHeapMap
from fruitbak.util.weak import weakproperty
from fruitbak.util.locking import lockeddescriptor
from fruitbak.pool.filesystem import Filesystem, LinuxFilesystem
from fruitbak.pool.agent import PoolAgent

class Pool(Clarity):
	def __init__(self, *args, **kwargs):
		self.lock = RLock()
		super().__init__(*args, **kwargs)

	max_queue_depth = 32
	queue_depth = 0

	@weakproperty
	def fruitbak(self):
		raise RuntimeError("pool.fruitbak used uninitialized")

	@initializer
	def config(self):
		return self.fruitbak.config

	@lockeddescriptor
	@initializer
	def root(self):
		assert self.locked
		return LinuxFilesystem(pool = self, config = self.config)

	@initializer
	def agents(self):
		assert self.locked
		return MinWeakHeapMap()

	next_agent_serial = 0

	@initializer
	def chunk_registry(self):
		assert self.locked
		return WeakValueDictionary()

	@property
	def locked(self):
		try:
			Condition(self.lock).notify()
		except RuntimeError:
			return False
		else:
			return True

	def exchange_chunk(self, hash, new_chunk = None):
		assert self.locked
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
		with self.lock:
			serial = self.next_agent_serial
			self.next_agent_serial = serial + 1
		return PoolAgent(pool = self, serial = serial, *args, **kwargs)

	def register_agent(self, agent):
		assert self.locked
		new = agent.avarice, agent.serial
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
		assert self.locked
		try:
			del self.agents[agent]
		except KeyError:
			pass

	def replenish_queue(self):
		assert self.locked
		agents = self.agents
		while agents and self.queue_depth < self.max_queue_depth:
			agent = agents.peekitem()[0]
			if agent is None:
				break
			serial = self.next_agent_serial
			self.next_agent_serial = serial + 1
			agent.serial = serial
			agent.dequeue()

	def get_chunk(self, callback, hash):
		return self.submit(self.root.get_chunk, callback, hash)

	def put_chunk(self, callback, hash, value):
		return self.submit(self.root.put_chunk, callback, hash, value)

	def del_chunk(self, callback, hash):
		return self.submit(self.root.del_chunk, callback, hash)

	def submit(self, func, callback, *args, **kwargs):
		lock = self.lock
		with lock:
			self.queue_depth += 1
		def when_done(*args, **kwargs):
			try:
				callback(*args, **kwargs)
			finally:
				with lock:
					self.queue_depth -= 1
					self.replenish_queue()
		return func(when_done, *args, **kwargs)

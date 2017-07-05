from weakref import ref as weakref, WeakValueDictionary

from warnings import warn
from threading import Condition, RLock
from collections import deque

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.heapmap import MinHeapMap
from fruitbak.pool.filesystem import Filesystem

class mapnode(weakref):
	__slots__ = ('hash', 'id')

	def __init__(self, agent, *args):
		super().__init__(agent, *args)
		self.hash = hash(agent)
		self.id = id(agent)

	def __hash__(self):
		return self.hash

	def __eq__(self, other):
		return self.id == other.id

poolresult = namedtuple(

class PoolAgent(Clarity):
	@initializer
	def cond(self):
		return Condition(self.pool.lock)

	@initializer
	def done(self):
		"""Completed operations"""
		return []

	@initializer
	def pending(self):
		"""The number of operations that have been submitted but not yet completed."""
		return 0

	@property
	def avarice(self):
		return len(self.done) + self.pending

	def dequeue(self):
		f = next(self.queue, None)
		if f is None:
			self.pool.unregister_agent(self)
		else:
			f(self)

	def update_registration(self):
		if self.queue:
			self.pool.register_agent(self)
		else:
			self.pool.unregister_agent(self)

	def queue_read(self, hash):
		"""Returns a function that will cause a read operation to be queued"""
		pool = self.pool
		with self.cond:
			value = pool.chunk_registry.get(hash)
			if value is not None:
				self.done.append({'hash': hash, 'value': value})
				self.cond.notify()
				def dummy(self):
					pass
				return dummy
				
		def read_operation(self):
			self.pending += 1
			self.update_registration()
			def whendone(hash, value, exception):
				with self.cond:
					value = pool.exchange_chunk(hash, value)
					self.pending -= 1
					self.done.append({'hash': hash, 'value': value, 'exception': exception})
					self.update_registration()
					self.cond.notify()
					pool.replenish_queue()
			pool.root.get_chunk(hash, whendone)
		return read_operation

	def queue_write(self, hash, value):
		"""Returns a function that will cause a write operation to be queued"""
		pool = self.pool
		with self.cond:
			value = pool.exchange_chunk(hash, value)
		def write_operation(self):
			self.pending += 1
			self.update_registration()
			def whendone(hash, exception):
				with self.cond:
					self.pending -= 1
					self.done.append({'hash': hash, 'value': value, 'exception': exception})
					self.update_registration()
					self.cond.notify()
					pool.replenish_queue()
			pool.root.put_chunk(hash, value, whendone)
		return write_operation

	def queue_delete(self, hash):
		"""Returns a function that will cause a delete operation to be queued"""
		pool = self.pool
		def delete_operation(self):
			self.pending += 1
			self.update_registration()
			def whendone(hash, value):
				with self.cond:
					self.pending -= 1
					self.done.append({'hash': hash, 'value': pool.chunk_registry.get(hash), 'exception': exception})
					self.update_registration()
					self.cond.notify()
					pool.replenish_queue()
			pool.root.del_chunk(hash, whendone)
		return delete_operation

	def wait(self):
		with self.cond:
			while self.pending and not self.done:
				self.cond.wait()
			try:
				return self.done.pop()
			except IndexError:
				return None

class Pool(Clarity):
	def __init__(self, *args, **kwargs):
		super().__init__(lock = RLock(), *args, **kwargs)

	@initializer
	def max_queue_depth(self):
		return 32

	@initializer
	def queue_depth(self):
		return 0

	@initializer
	def root(self):
		return Filesystem(cfg = self.cfg)

	@initializer
	def agents(self):
		return MinHeapMap()

	@initializer
	def chunk_registry(self):
		return WeakValueDictionary()

	def exchange_chunk(self, hash, new_chunk = None):
		# can't use setdefault(), it has weird corner cases
		# involving None
		chunk_registry = self.chunk_registry
		old_chunk = chunk_registry.get(hash)
		if old_chunk is not None:
			return old_chunk
		if new_chunk is not None:
			chunk_registry[hash] = new_chunk
		return new_chunk

	def agent(self, *args, **kwargs):
		return PoolAgent(pool = self, *args, **kwargs)

	def select_most_modest_agent(self):
		agents = self.agents
		while agents:
			agentref = agents.peek()
			agent = agentref()
			if agent is None:
				try:
					del agents[agentref]
				except KeyError:
					pass
			else:
				return agent

	def register_agent(self, agent):
		agents = self.agents
		try:
			del agents[mapnode(agent)]
		except KeyError:
			pass
		node = None
		def finalizer(r):
			try:
				del agents[node]
			except Exception as e:
				warn(e)
		node = mapnode(agent, finalizer)
		agents[node] = agent.avarice

	def unregister_agent(self, agent):
		try:
			del self.agents[mapnode(agent)]
		except KeyError:
			pass

	def replenish_queue(self):
		while len(self.agents.heap) and self.queue_depth < self.max_queue_depth:
			agent = self.select_most_modest_agent()
			if agent is None:
				break
			agent.dequeue()

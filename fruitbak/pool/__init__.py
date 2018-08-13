from weakref import ref as weakref, WeakValueDictionary

from warnings import warn
from threading import Condition, RLock
from collections import deque

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.heapmap import MinHeapMap
from fruitbak.pool.filesystem import Filesystem

class fakemapnode(object):
	__slots__ = ('hash', 'id')

	def __init__(self, agent):
		self.hash = hash(agent)
		self.id = id(agent)

	def __hash__(self):
		return self.hash

	def __eq__(self, other):
		return self.id == other.id

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

class weakbytes(bytes):
	"""TypeError: cannot create weak reference to 'bytes' object"""
	pass

class PoolAction(Clarity):
	@initializer
	def done(self):
		return False

	@initializer
	def exception(self):
		return None

class PoolReadAction(PoolAction):
	pass

class PoolWriteAction(PoolAction):
	pass

class PoolDeleteAction(PoolAction):
	pass

class PoolAgent(Clarity):
	@initializer
	def cond(self):
		return Condition(self.pool.lock)

	@initializer
	def queue(self):
		"""Submitted readahead operations that may or may not have completed"""
		return deque()

	@initializer
	def pending(self):
		return MinHeapMap()

	@initializer
	def serial(self):
		return 0

	@property
	def avarice(self):
		if self.readahead is None:
			return None
		else:
			return len(self.queue)

	def dequeue(self):
		readahead = self.readahead
		if readahead is None:
			return
		pool = self.pool
		hash = next(readahead, None)
		if hash is None:
			self.readahead = None
			pool.unregister_agent(self)
		else:
			self.update_registration()
			cond = self.cond
			action = PoolReadAction(hash = hash)
			self.queue.append(action)
			value = pool.chunk_registry.get(hash)
			self.queue.append(action)
			if value is None:
				def when_done(value, exception):
					with cond:
						action.value = value
						action.exception = exception
						action.done = True
						cond.notify()
				pool.get_chunk(hash, when_done)
			else:
				action.value = value
				action.done = True
				cond.notify()

			return action

	def update_registration(self):
		pool = self.pool
		if self.readahead:
			pool.register_agent(self)
		else:
			pool.unregister_agent(self)

	def put_chunk(self, hash, value, async = False):
		pending = self.pending
		cond = self.cond
		with cond:
			serial = self.serial
			self.serial = serial + 1
			action = PoolWriteAction(hash = hash, value = value)
			def when_done(exception):
				action.done = True
				if exception is None:
					with cond:
						del pending[action]
						cond.notify()
				else:
					action.exception = exception
					with cond:
						cond.notify()
			pending[action] = serial
			self.pool.put_chunk(hash, value, when_done)
			if not async:
				while not action.done:
					cond.wait()
				if action.exception:
					raise action.exception
			return action

	def sync(self):
		serial = self.serial
		pending = self.pending
		cond = self.cond:
		with cond:
			while pending && pending.peek() < serial:
				cond.wait()

	def wait(self):
		self.update_registration()
		cond = self.cond
		pool = self.pool
		with cond:
			queue = self.queue
			while (queue and not queue[0].done) or self.readahead:
				pool.replenish_queue()
				cond.wait()
			try:
				return queue.popleft()
			except IndexError:
				return None
			finally:
				self.update_registration()

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
	def config(self):
		return self.fbak.config

	@initializer
	def root(self):
		return Filesystem(config = self.config)

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
			try:
				chunk_registry[hash] = new_chunk
			except TypeError:
				new_chunk = memoryview(new_chunk)
				chunk_registry[hash] = new_chunk
		return new_chunk

	def agent(self, *args, **kwargs):
		return PoolAgent(pool = self, *args, **kwargs)

	def select_most_modest_agent(self):
		agents = self.agents
		while agents:
			agentref = agents.peekitem()[0]
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
		def finalizer(r):
			try:
				del agents[r]
			except KeyError:
				pass
			except Exception as e:
				warn(e)
		node = mapnode(agent, finalizer)
		agents[node] = agent.avarice

	def unregister_agent(self, agent):
		try:
			del self.agents[fakemapnode(agent)]
		except KeyError:
			pass

	def replenish_queue(self):
		while len(self.agents.heap) and self.queue_depth < self.max_queue_depth:
			agent = self.select_most_modest_agent()
			if agent is None:
				break
			agent.dequeue()

	def get_chunk(self, hash, callback):
		lock = self.lock
		with lock:
			self.queue_depth += 1
		def when_done(value, exception):
			with lock:
				self.queue_depth -= 1
				self.replenish_queue()
			return callback(value, exception)
		return self.root.get_chunk(hash, callback)

	def put_chunk(self, hash, value, callback):
		lock = self.lock
		with lock:
			self.queue_depth += 1
		def when_done(hash, value, exception):
			with lock:
				self.queue_depth -= 1
				self.replenish_queue()
			return callback(exception)
		return self.root.put_chunk(hash, value, callback)

	def del_chunk(self, hash, callback):
		lock = self.lock
		with lock:
			self.queue_depth += 1
		def when_done(hash, value, exception):
			with lock:
				self.queue_depth -= 1
				self.replenish_queue()
			return callback(exception)
		return self.root.del_chunk(hash, callback)

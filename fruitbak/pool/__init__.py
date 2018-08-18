from weakref import ref as weakref, WeakValueDictionary

from warnings import warn
from threading import Condition, RLock
from collections import deque
from sys import stderr

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
	done = False
	cond = None
	hash = None
	value = None
	exception = None

	def sync(self):
		cond = self.cond
		with cond:
			while not self.done:
				cond.wait()
		exception = self.exception
		if self.exception:
			raise exception[1]

class PoolReadAction(PoolAction):
	def sync(self):
		super().sync()
		return self.value

class PoolWriteAction(PoolAction):
	pass

class PoolDeleteAction(PoolAction):
	pass

class PoolAgent(Clarity):
	@initializer
	def cond(self):
		return Condition(self.pool.lock)

	@initializer
	def pending_readaheads(self):
		"""Submitted readahead operations that may or may not have completed"""
		return deque()

	# Iterator that yields chunks to prefetch
	readahead = None

	# Number of submitted read operations that have not yet completed
	incomplete_reads = 0

	# Number of submitted readahead operations that have not yet completed
	incomplete_readaheads = 0

	@initializer
	def pending_operations(self):
		"""Directly submitted operations that have not completed"""
		return MinHeapMap()

	# A function that will queue a direct operation
	mailhook = None

	# This agent's serial
	serial = None

	# The next serial to assign to an action
	next_serial = 0

	# The last exception that was raised
	exception = None

	@property
	def avarice(self):
		if self.mailhook:
			return len(self.pending_operations) + self.incomplete_reads + self.incomplete_readaheads
		else:
			return len(self.pending_operations) + self.incomplete_reads + len(self.pending_readaheads)

	def dequeue(self):
		pool = self.pool
		
		op = self.mailhook
		if op:
			self.mailhook = None
			op()
			return

		if self.pending_operations:
			pool.unregister_agent(self)
			return

		readahead = self.readahead
		if readahead is None:
			pool.unregister_agent(self)
			return

		hash = next(readahead, None)
		if hash is None:
			self.readahead = None
			pool.unregister_agent(self)
			return

		cond = self.cond
		action = PoolReadAction(hash = hash)
		self.pending_readaheads.append(action)
		pool.register_agent(self)
		value = pool.chunk_registry.get(hash)
		if value is None:
			def when_done(value, exception):
				with cond:
					action.value = value
					action.exception = exception
					action.done = True
					self.incomplete_readaheads -= 1
					cond.notify()
			self.incomplete_readaheads += 1
			pool.get_chunk(hash, when_done)
		else:
			action.value = value
			action.done = True
			cond.notify()

	def update_registration(self):
		pool = self.pool
		if self.mailhook or (self.readahead and not self.pending_operations):
			pool.register_agent(self)
		else:
			pool.unregister_agent(self)

	def get_chunk(self, hash, async = False):
		cond = self.cond
		pool = self.pool
		with cond:
			if self.mailhook:
				raise Exception("operation already in progress")

			action = PoolReadAction(hash = hash, cond = cond)
			def when_done(value, exception):
				with cond:
					if exception:
						action.exception = exception
						self.exception = exception
					else:
						action.value = value
					self.incomplete_reads -= 1
					action.done = True
					cond.notify()

			def mailbag():
				self.incomplete_reads += 1
				self.update_registration()
				pool.get_chunk(hash, when_done)

			self.mailhook = mailbag
			pool.register_agent(self)
			pool.replenish_queue()

			while self.mailhook is mailbag:
				cond.wait()

			if async:
				return action

			return action.sync()

	def put_chunk(self, hash, value, async = False):
		cond = self.cond
		pool = self.pool
		with cond:
			if self.mailhook:
				raise Exception("operation already in progress")

			if self.exception:
				raise Exception("an operation has failed. call agent.sync() first") from self.exception
			
			action = PoolWriteAction(hash = hash, value = value, cond = cond)
			def when_done(exception):
				with cond:
					del self.pending_operations[action]
					self.update_registration()
					if exception:
						action.exception = exception
						self.exception = exception
					action.done = True
					cond.notify()

			def mailbag():
				serial = self.next_serial
				self.next_serial = serial + 1
				self.pending_operations[action] = serial
				self.update_registration()
				pool.put_chunk(hash, value, when_done)

			self.mailhook = mailbag
			pool.register_agent(self)
			pool.replenish_queue()

			while self.mailhook is mailbag:
				cond.wait()

			if async:
				return action

			action.sync()

	def del_chunk(self, hash, value, async = False):
		cond = self.cond
		pool = self.pool
		with cond:
			if self.mailhook:
				raise Exception("operation already in progress")

			if self.exception:
				raise Exception("an operation has failed. call agent.sync() first") from self.exception

			action = PoolDeleteAction(hash = hash, value = value, cond = cond)
			def when_done(exception):
				with cond:
					del self.pending_operations[action]
					self.update_registration()
					if exception:
						action.exception = exception
						self.exception = exception
					action.done = True
					cond.notify()

			def mailbag():
				serial = self.next_serial
				self.next_serial = serial + 1
				self.pending_operations[action] = serial
				self.update_registration()
				pool.del_chunk(hash, value, when_done)

			self.mailhook = mailbag
			pool.register_agent(self)
			pool.replenish_queue()

			while self.mailhook is mailbag:
				cond.wait()

			if async:
				return action

			action.sync()

	def sync(self):
		pending_operations = self.pending_operations
		cond = self.cond
		with cond:
			serial = self.next_serial
			while pending_operations and pending_operations.peek() < serial:
				cond.wait()
			exception = self.exception
			self.exception = None
			if exception:
				raise Exception("an operation has failed") from exception

	def __iter__(self):
		return self

	def __next__(self):
		cond = self.cond
		pool = self.pool
		with cond:
			self.update_registration()
			pool.replenish_queue()
			pending_readaheads = self.pending_readaheads
			while self.readahead or (pending_readaheads and not pending_readaheads[0].done):
				cond.wait()
			try:
				return pending_readaheads.popleft()
			except IndexError:
				raise StopIteration()
			finally:
				self.update_registration()

class Pool(Clarity):
	def __init__(self, *args, **kwargs):
		super().__init__(lock = RLock(), *args, **kwargs)

	max_queue_depth = 32
	queue_depth = 0

	@initializer
	def config(self):
		return self.fbak.config

	@initializer
	def root(self):
		return Filesystem(config = self.config)

	@initializer
	def agents(self):
		return MinHeapMap()

	next_serial = 0

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
		with self.lock:
			serial = self.next_serial
			self.next_serial = serial + 1
		return PoolAgent(pool = self, serial = serial, *args, **kwargs)

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
				serial = self.next_serial
				self.next_serial = serial + 1
				agent.serial = serial
				return agent

	def register_agent(self, agent):
		agents = self.agents
		def finalizer(r):
			try:
				del agents[r]
			except KeyError:
				pass
			except:
				print_exc(file = stderr)
		node = mapnode(agent, finalizer)
		agents[node] = agent.avarice, agent.serial

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
			try:
				callback(value, exception)
			finally:
				with lock:
					self.queue_depth -= 1
					self.replenish_queue()
		return self.root.get_chunk(hash, when_done)

	def put_chunk(self, hash, value, callback):
		lock = self.lock
		with lock:
			self.queue_depth += 1
		def when_done(exception):
			try:
				callback(exception)
			finally:
				with lock:
					self.queue_depth -= 1
					self.replenish_queue()
		return self.root.put_chunk(hash, value, when_done)

	def del_chunk(self, hash, callback):
		lock = self.lock
		with lock:
			self.queue_depth += 1
		def when_done(exception):
			try:
				callback(exception)
			finally:
				with lock:
					self.queue_depth -= 1
					self.replenish_queue()
		return self.root.del_chunk(hash, when_done)

from weakref import ref as weakref, WeakValueDictionary

from warnings import warn
from threading import Condition, RLock
from collections import deque
from sys import stderr

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.heapmap import MinHeapMap
from fruitbak.util.weakheapmap import MinWeakHeapMap
from fruitbak.pool.filesystem import Filesystem
	
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

class PoolReadahead(Clarity):
	agent = None
	iterator = None
	serial = None

	@initializer
	def cond(self):
		return self.agent.cond

	@initializer
	def pool(self):
		return self.agent.pool

	@initializer
	def queue(self):
		return deque()

	@property
	def spent(self):
		return self.iterator is None

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.agent.register_readahead(self)

	def __len__(self):
		return len(self.queue)

	def __bool__(self):
		return bool(self.queue)

	def __iter__(self):
		return self

	def __next__(self):
		cond = self.cond
		queue = self.queue
		print(len(queue))
		with cond:
			while not queue[0].done if queue else self.iterator:
				cond.wait()
			try:
				return queue.popleft()
			except IndexError:
				raise StopIteration()
			finally:
				self.agent.total_readaheads -= 1
				self.agent.register_readahead(self)

	def __del__(self):
		agent = self.agent
		if agent is not None:
			agent.unregister_readahead(self)

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		self.agent.unregister_readahead(self)
		self.__dict__.clear()

	def dequeue(self):
		iterator = self.iterator
		if iterator is None:
			self.agent.register_readahead(self)
			return
		hash = next(iterator, None)
		if hash is None:
			self.iterator = None
			self.agent.register_readahead(self)
			return

		cond = self.cond
		action = PoolReadAction(hash = hash)
		agent = self.agent
		self.queue.append(action)
		agent.register_readahead(self)

		pool = self.pool
		value = pool.chunk_registry.get(hash)
		if value is None:
			def when_done(value, exception):
				with cond:
					action.value = value
					action.exception = exception
					action.done = True
					agent.pending_readaheads -= 1
					agent.register_readahead(self)
					cond.notify()
			self.agent.pending_readaheads += 1
			self.agent.total_readaheads += 1
			pool.get_chunk(hash, when_done)
		else:
			action.value = value
			action.done = True
			cond.notify()

class PoolAgent(Clarity):
	@initializer
	def cond(self):
		return Condition(self.pool.lock)

	# Number of submitted read actions that have not yet completed
	pending_reads = 0

	@initializer
	def pending_writes(self):
		"""Submitted write/delete actions that have not yet completed"""
		return MinHeapMap()

	# A function that will queue a direct action
	mailhook = None

	@initializer
	def readaheads(self):
		return MinWeakHeapMap()

	# Sum of the length of all readaheads, both completed and not yet completed
	total_readaheads = 0

	# Number of submitted readahead actions that have not yet completed
	pending_readaheads = 0

	# Maximum number of readahead actions
	max_readaheads = 32

	# This agent's serial
	serial = None

	# The next serial to assign to an action
	next_action_serial = 0

	# The next serial to assign to a readahead object
	next_readahead_serial = 0

	# The last exception that was raised
	exception = None

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		self.sync()
		self.pool.unregister_agent(self)
		self.__dict__.clear()

	def readahead(self, iterator):
		return PoolReadahead(agent = self, iterator = iterator)

	@property
	def avarice(self):
		pending_writes = self.pending_writes
		pending_reads = self.pending_reads
		if self.mailhook or pending_writes or pending_reads:
			return len(pending_writes) + pending_reads + self.pending_readaheads

		try:
			spent, length, serial = self.readaheads.peek()
		except IndexError:
			return self.pending_readaheads

		if spent or length:
			return max(self.total_readaheads, self.pending_readaheads)

		return self.pending_readaheads

	@property
	def eligible_readahead(self):
		try:
			readahead, (spent, length, serial) = self.readaheads.peekitem()
		except IndexError:
			return None

		if spent or (length and self.total_readaheads >= self.max_readaheads):
			return None

		return readahead

	def dequeue(self):
		pool = self.pool

		op = self.mailhook
		if op:
			self.mailhook = None
			try:
				op()
			finally:
				pool.unregister_agent(self)
			return

		if self.pending_writes or self.pending_reads:
			pool.unregister_agent(self)
			return

		readahead = self.eligible_readahead
		if readahead is None:
			pool.unregister_agent(self)
		else:
			readahead.dequeue()

	def register_readahead(self, readahead):
		readaheads = self.readaheads
		try:
			spent, length, serial = readaheads[readahead]
		except KeyError:
			length = 0
		self.total_readaheads -= length
		serial = readahead.serial
		if serial is None:
			serial = self.next_readahead_serial
			self.next_readahead_serial = serial + 1
			readahead.serial = serial
		readaheads[readahead] = readahead.spent, len(readahead), serial
		self.total_readaheads += len(readahead)
		self.update_registration()

	def unregister_readahead(self, readahead):
		readaheads = self.readaheads
		try:
			spent, length, serial = readaheads[readahead]
			del readaheads[readahead]
		except KeyError:
			length = 0
		self.total_readaheads -= length
		self.update_registration()

	def update_registration(self):
		pool = self.pool
		if self.mailhook:
			pool.register_agent(self)
		elif self.pending_reads or self.pending_writes:
			pool.unregister_agent(self)
		elif self.eligible_readahead is None:
			pool.unregister_agent(self)
		else:
			pool.register_agent(self)
		pool.replenish_queue()

	def get_chunk(self, hash, async = False):
		cond = self.cond
		pool = self.pool
		with cond:
			if self.mailhook:
				raise Exception("action already in progress")

			action = PoolReadAction(hash = hash, cond = cond)
			def when_done(value, exception):
				with cond:
					if exception:
						action.exception = exception
						self.exception = exception
					else:
						action.value = value
					self.pending_reads -= 1
					action.done = True
					cond.notify()

			def mailbag():
				self.pending_reads += 1
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
				raise Exception("action already in progress")

			if self.exception:
				raise Exception("an operation has failed. call agent.sync() first") from self.exception
			
			action = PoolWriteAction(hash = hash, value = value, cond = cond)
			def when_done(exception):
				with cond:
					del self.pending_writes[action]
					self.update_registration()
					if exception:
						action.exception = exception
						self.exception = exception
					action.done = True
					cond.notify()

			def mailbag():
				serial = self.next_action_serial
				self.next_action_serial = serial + 1
				self.pending_writes[action] = serial
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
				raise Exception("action already in progress")

			if self.exception:
				raise Exception("an operation has failed. call agent.sync() first") from self.exception

			action = PoolDeleteAction(hash = hash, value = value, cond = cond)
			def when_done(exception):
				with cond:
					del self.pending_writes[action]
					self.update_registration()
					if exception:
						action.exception = exception
						self.exception = exception
					action.done = True
					cond.notify()

			def mailbag():
				serial = self.next_action_serial
				self.next_action_serial = serial + 1
				self.pending_writes[action] = serial
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
		pending_writes = self.pending_writes
		cond = self.cond
		with cond:
			serial = self.next_action_serial
			while pending_writes and pending_writes.peek() < serial:
				cond.wait()
			exception = self.exception
			self.exception = None
			if exception:
				raise Exception("an operation has failed") from exception

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
		return MinWeakHeapMap()

	next_agent_serial = 0

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
			serial = self.next_agent_serial
			self.next_agent_serial = serial + 1
		return PoolAgent(pool = self, serial = serial, *args, **kwargs)

	def register_agent(self, agent):
		self.agents[agent] = agent.avarice, agent.serial

	def unregister_agent(self, agent):
		try:
			del self.agents[agent]
		except KeyError:
			pass

	def replenish_queue(self):
		agents = self.agents
		while agents and self.queue_depth < self.max_queue_depth:
			agent = agents.peekitem()[0]
			if agent is None:
				break
			serial = self.next_agent_serial
			self.next_agent_serial = serial + 1
			agent.serial = serial
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

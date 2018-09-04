from weakref import ref as weakref, WeakValueDictionary

from threading import Condition
from collections import deque
from sys import stderr

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.heapmap import MinHeapMap
from fruitbak.util.weakheapmap import MinWeakHeapMap
from fruitbak.util.locking import lockeddescriptor

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
	def lock(self):
		return self.agent.lock

	@initializer
	def cond(self):
		return self.agent.cond

	@initializer
	def pool(self):
		return self.agent.pool

	@lockeddescriptor
	@initializer
	def queue(self):
		return deque()

	@property
	def spent(self):
		return self.iterator is None

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		with self.cond:
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
		with cond:
			while not queue[0].done if queue else self.iterator:
				cond.wait()
			try:
				return queue.popleft()
			except IndexError:
				raise StopIteration()
			finally:
				agent = self.agent
				agent.register_readahead(self)

	def __del__(self):
		agent = self.agent
		if agent is not None:
			with self.cond:
				agent.unregister_readahead(self)

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		self.agent.unregister_readahead(self)
		self.__dict__.clear()

	def submit(self, func, callback, *args, **kwargs):
		cond = self.cond
		agent = self.agent
		def when_done(*args, **kwargs):
			with cond:
				try:
					callback(*args, **kwargs)
				finally:
					agent.pending_readaheads -= 1
					agent.register_readahead(self)
					cond.notify()
		agent.pending_readaheads += 1
		func(when_done, *args, **kwargs)

	def dequeue(self):
		agent = self.agent
		cond = self.cond

		iterator = self.iterator
		if iterator is None:
			agent.register_readahead(self)
			return

		hash = next(iterator, None)
		if hash is None:
			self.iterator = None
			agent.register_readahead(self)
			return

		action = PoolReadAction(hash = hash)
		self.queue.append(action)

		pool = self.pool
		value = pool.chunk_registry.get(hash)
		if value is None:
			def when_done(value, exception):
				action.value = value
				action.exception = exception
				action.done = True
			self.submit(pool.get_chunk, when_done, hash)
		else:
			action.value = value
			action.done = True
			cond.notify()

		agent.register_readahead(self)

class PoolAgent(Clarity):
	@initializer
	def lock(self):
		return self.pool.lock

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
		with self.cond:
			self.pool.unregister_agent(self)
		self.__dict__.clear()

	def readahead(self, iterator):
		assert self.pool.locked
		return PoolReadahead(agent = self, iterator = iterator)

	@property
	def avarice(self):
		assert self.pool.locked
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
		assert self.pool.locked
		try:
			readahead, (spent, length, serial) = self.readaheads.peekitem()
		except IndexError:
			return None

		if spent or (length and self.total_readaheads >= self.max_readaheads):
			return None

		return readahead

	def dequeue(self):
		pool = self.pool
		assert pool.locked

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
		assert self.pool.locked
		new_serial = readahead.serial
		if new_serial is None:
			new_serial = self.next_readahead_serial
			self.next_readahead_serial = new_serial + 1
			readahead.serial = new_serial
		new_length = len(readahead)
		new = readahead.spent, new_length, new_serial

		readaheads = self.readaheads
		try:
			old = readaheads[readahead]
		except KeyError:
			old_length = 0
		else:
			if old == new:
				return
			old_spent, old_length, old_serial = old
			self.total_readaheads -= old_length

		readaheads[readahead] = new
		self.total_readaheads += new_length
		self.update_registration()

	def unregister_readahead(self, readahead):
		assert self.pool.locked
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
		assert pool.locked
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
				raise RuntimeError("action already in progress")

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
				pool.get_chunk(when_done, hash)

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
				raise RuntimeError("action already in progress")

			if self.exception:
				raise RuntimeError("an operation has failed. call agent.sync() first") from self.exception[1]
			
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
				pool.put_chunk(when_done, hash, value)

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
				raise RuntimeError("action already in progress")

			if self.exception:
				raise RuntimeError("an operation has failed. call agent.sync() first") from self.exception[1]

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
				pool.del_chunk(when_done, hash, value)

			self.mailhook = mailbag
			pool.register_agent(self)
			pool.replenish_queue()

			while self.mailhook is mailbag:
				cond.wait()

			if async:
				return action

			action.sync()

	def lister(self):
		return self.pool.root.lister(self)

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
				raise RuntimeError("an operation has failed") from exception

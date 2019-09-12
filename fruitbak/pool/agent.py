from weakref import ref as weakref, WeakValueDictionary

from threading import Condition
from collections import deque, OrderedDict
from sys import stderr

from fruitbak.util import Initializer, initializer, MinHeapMap, MinWeakHeapMap
from fruitbak.config import configurable

class PoolAction(Initializer):
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

class PoolGetAction(PoolAction):
	def sync(self):
		super().sync()
		return self.value

class PoolHasAction(PoolAction):
	def sync(self):
		super().sync()
		return self.value

class PoolPutAction(PoolAction):
	pass

class PoolDelAction(PoolAction):
	pass

class PoolReadahead(Initializer):
	agent = None
	iterator = None

	@initializer
	def lock(self):
		return self.agent.lock

	@initializer
	def cond(self):
		return self.agent.cond

	@initializer
	def pool(self):
		return self.agent.pool

	@initializer
	def queue(self):
		assert self.lock
		return deque()

	@property
	def spent(self):
		return self.iterator is None

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		with self.lock:
			self.iterator = iter(self.iterator)
			self.agent.register_readahead(self)

	def __len__(self):
		return len(self.queue)

	def __bool__(self):
		return bool(self.queue)

	def __iter__(self):
		return self

	def __next__(self):
		with self.lock:
			cond = self.cond
			queue = self.queue

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
			with self.lock:
				agent.unregister_readahead(self)

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		with self.lock:
			self.agent.unregister_readahead(self)
		vars(self).clear()

	def submit(self, func, callback, *args, **kwargs):
		assert self.lock
		cond = self.cond
		agent = self.agent
		def when_done(*args, **kwargs):
			with cond:
				try:
					callback(*args, **kwargs)
				finally:
					agent.pending_readaheads -= 1
					agent.register_readahead(self)
					cond.notify_all()
		agent.pending_readaheads += 1
		func(when_done, *args, **kwargs)

	def dequeue(self):
		assert self.lock
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

		action = PoolGetAction(hash = hash)
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
			cond.notify_all()

		agent.register_readahead(self)

class PoolAgent(Initializer):
	@initializer
	def lock(self):
		return self.pool.lock

	@initializer
	def cond(self):
		assert self.lock
		return Condition(self.lock)

	@initializer
	def config(self):
		return self.pool.config

	# Number of submitted read actions that have not yet completed
	pending_reads = 0

	@initializer
	def pending_writes(self):
		"""Submitted write/delete actions that have not yet completed"""
		assert self.lock
		return MinHeapMap()

	# A function that will queue a direct action
	@initializer
	def mailhook(self):
		assert self.lock
		return OrderedDict()

	@initializer
	def readaheads(self):
		assert self.lock
		return MinWeakHeapMap()

	# Sum of the length of all readaheads, both completed and not yet completed
	total_readaheads = 0

	# Number of submitted readahead actions that have not yet completed
	pending_readaheads = 0

	# Maximum number of readahead actions
	@configurable('pool_max_readaheads')
	def max_readaheads(self):
		return 32

	# The next serial to assign to an action
	next_action_serial = 0

	# The last exception that was raised
	exception = None

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		self.sync()
		with self.lock:
			self.pool.unregister_agent(self)
		vars(self).clear()

	def readahead(self, iterator):
		return PoolReadahead(agent = self, iterator = iterator)

	@property
	def avarice(self):
		assert self.lock
		pending_writes = self.pending_writes
		pending_reads = self.pending_reads
		if self.mailhook or pending_writes or pending_reads:
			return len(pending_writes) + pending_reads + self.pending_readaheads

		try:
			spent, length = self.readaheads.peek()
		except IndexError:
			return self.pending_readaheads

		if spent or length:
			return max(self.total_readaheads, self.pending_readaheads)

		return self.pending_readaheads

	@property
	def eligible_readahead(self):
		assert self.lock
		try:
			readahead, (spent, length) = self.readaheads.peekitem()
		except IndexError:
			return None

		if spent or (length and self.total_readaheads >= self.max_readaheads):
			return None

		return readahead

	def dequeue(self):
		assert self.lock
		pool = self.pool

		try:
			op, dummy = self.mailhook.popitem()
		except KeyError:
			pass
		else:
			self.cond.notify_all()
			op()
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
		assert self.lock
		new_length = len(readahead)
		new = readahead.spent, new_length

		readaheads = self.readaheads
		try:
			old = readaheads[readahead]
		except KeyError:
			old_length = 0
		else:
			if old == new:
				return
			old_spent, old_length = old
			self.total_readaheads -= old_length

		readaheads[readahead] = new
		self.total_readaheads += new_length
		self.update_registration()

	def unregister_readahead(self, readahead):
		assert self.lock
		readaheads = self.readaheads
		try:
			spent, length = readaheads.pop(readahead)
		except KeyError:
			length = 0
		self.total_readaheads -= length
		self.update_registration()

	def update_registration(self):
		assert self.lock
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

	def has_chunk(self, hash, wait = True):
		lock = self.lock
		with lock:
			cond = self.cond
			pool = self.pool

			action = PoolHasAction(hash = hash, cond = cond)
			def when_done(value, exception):
				with lock:
					if exception:
						action.exception = exception
						self.exception = exception
					else:
						action.value = value
					self.pending_reads -= 1
					action.done = True
					cond.notify_all()

			def mailbag():
				self.pending_reads += 1
				self.update_registration()
				pool.has_chunk(when_done, hash)

			mailhook = self.mailhook
			mailhook[mailbag] = None
			pool.register_agent(self)
			pool.replenish_queue()

			while mailbag in self.mailhook:
				cond.wait()

			if not wait:
				return action

			return action.sync()

	def get_chunk(self, hash, wait = True):
		lock = self.lock
		with lock:
			cond = self.cond
			pool = self.pool

			action = PoolGetAction(hash = hash, cond = cond)
			def when_done(value, exception):
				with lock:
					if exception:
						action.exception = exception
						self.exception = exception
					else:
						action.value = value
					self.pending_reads -= 1
					action.done = True
					cond.notify_all()

			def mailbag():
				self.pending_reads += 1
				self.update_registration()
				pool.get_chunk(when_done, hash)

			mailhook = self.mailhook
			mailhook[mailbag] = None
			pool.register_agent(self)
			pool.replenish_queue()

			while mailbag in self.mailhook:
				cond.wait()

		if not wait:
			return action

		return action.sync()

	def put_chunk(self, hash, value, wait = True):
		lock = self.lock
		with lock:
			cond = self.cond
			pool = self.pool

			if self.exception:
				raise RuntimeError("an operation has failed. call agent.sync() first") from self.exception[1]
			
			action = PoolPutAction(hash = hash, value = value, cond = cond)
			def when_done(exception):
				with lock:
					del self.pending_writes[action]
					self.update_registration()
					if exception:
						action.exception = exception
						self.exception = exception
					action.done = True
					cond.notify_all()

			def mailbag():
				serial = self.next_action_serial
				self.next_action_serial = serial + 1
				self.pending_writes[action] = serial
				self.update_registration()
				pool.put_chunk(when_done, hash, value)

			mailhook = self.mailhook
			mailhook[mailbag] = None
			pool.register_agent(self)
			pool.replenish_queue()

			while mailbag in self.mailhook:
				cond.wait()

		if not wait:
			return action

		action.sync()

	def del_chunk(self, hash, wait = True):
		lock = self.lock
		with lock:
			cond = self.cond
			pool = self.pool

			if self.exception:
				raise RuntimeError("an operation has failed. call agent.sync() first") from self.exception[1]

			action = PoolDelAction(hash = hash, cond = cond)
			def when_done(exception):
				with lock:
					del self.pending_writes[action]
					self.update_registration()
					if exception:
						action.exception = exception
						self.exception = exception
					action.done = True
					cond.notify_all()

			def mailbag():
				serial = self.next_action_serial
				self.next_action_serial = serial + 1
				self.pending_writes[action] = serial
				self.update_registration()
				pool.del_chunk(when_done, hash)

			mailhook = self.mailhook
			mailhook[mailbag] = None
			pool.register_agent(self)
			pool.replenish_queue()

			while mailbag in self.mailhook:
				cond.wait()

		if not wait:
			return action

		action.sync()

	def lister(self):
		with self.lock:
			return self.pool.root.lister(self)

	def sync(self):
		with self.lock:
			cond = self.cond
			pending_writes = self.pending_writes
			serial = self.next_action_serial
			while pending_writes and pending_writes.peek() < serial:
				cond.wait()
			exception = self.exception
			self.exception = None
			if exception:
				raise RuntimeError("an operation has failed") from exception

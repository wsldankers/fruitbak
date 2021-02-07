from fruitbak.util import initializer, NCondition
from fruitbak.pool.storage import Storage
from fruitbak.config import configurable

from traceback import print_exc
from sys import stderr, exc_info
from threading import Thread
from collections import deque

class _Mutable:
	__slots__ = 'value',

	def __init__(self, value = None):
		self.value = value

	def __bool__(self):
		return bool(self.value)

	def __len__(self):
		return len(self.value)

	def __str__(self):
		return str(self.value)

class _WriteJob:
	__slots__ = 'operation', 'callback', 'exception'

	def __init__(self, operation, callback, exception = None):
		self.operation = operation
		self.callback = callback
		self.exception = exception

class _Worker(Thread):
	def __init__(self, cond, reads, writes, writing, env, done, *args, **kwargs):
		super().__init__(*args, daemon = True, **kwargs)

		self.cond = cond
		self.reads = reads
		self.writes = writes
		self.writing = writing
		self.env = env
		self.done = done

		self.start()

	def run(self):
		cond = self.cond
		reads = self.reads
		writes = self.writes
		writing = self.writing
		env = self.env
		done = self.done

		try:
			while True:
				with cond:
					while not done and not reads and (not writes or writing):
						cond.wait()
					if done:
						break
					elif writes and not writing:
						accepted_writes = writes.value
						writes.value = deque()
						writing.value = True

						def job():
							nonlocal accepted_writes
							while True:
								results = []

								# perform all operations:
								try:
									with env.write_batch(sync = True) as txn:
										while accepted_writes:
											job = accepted_writes.popleft()
											results.append(job)
											operation = job.operation
											try:
												operation(txn)
											except:
												job.exception = exc_info()
								except:
									txn_exception = exc_info()
								else:
									txn_exception = None

								# call all calbacks:
								for job in results:
									callback = job.callback
									try:
										callback(job.exception or txn_exception)
									except:
										print_exc()

								del results

								if not accepted_writes:
									# see if there's more to do:
									with cond:
										if writes:
											accepted_writes = writes.value
											writes.value = deque()
										else:
											writing.value = False
											break
					elif reads:
						job = reads.popleft()

				job()
		except:
			print_exc()

class Plyvel(Storage):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.cond = NCondition()

	@configurable
	def pooldir(self):
		return 'pool'

	@pooldir.prepare
	def pooldir(self, value):
		return self.fruitbak.rootdir / value

	@initializer
	def _ensure_started(self):
		cond = self.cond
		assert cond

		done = self.done

		try:
			workers = [_Worker(cond, self.reads, self.writes, self.writing, self.env, done) for x in range(self.max_workers)]
		except:
			done.value = True
			cond.notify_all()
			raise
		else:
			return workers

	@initializer
	def reads(self):
		assert self.cond
		return deque()

	@initializer
	def writes(self):
		assert self.cond
		return _Mutable(deque())

	@initializer
	def writing(self):
		assert self.cond
		return _Mutable()

	@initializer
	def env(self):
		assert self.cond
		from plyvel import DB, IteratorInvalidError
		self.IteratorInvalidError = IteratorInvalidError
		return DB(bytes(self.pooldir), create_if_missing = True, compression = None)

	@initializer
	def IteratorInvalidError(self):
		from plyvel import IteratorInvalidError
		return IteratorInvalidError

	@initializer
	def done(self):
		assert self.cond
		return _Mutable()

	def has_chunk(self, callback, hash):
		hash = bytes(hash)

		cond = self.cond
		with cond:
			env = self.env
		IteratorInvalidError = self.IteratorInvalidError

		def job():
			try:
				with env.raw_iterator() as i:
					i.seek(hash)
					try:
						result = i.key() == hash
					except IteratorInvalidError:
						result = False
			except:
				callback(None, exc_info())
			else:
				callback(result, None)

		with cond:
			self._ensure_started
			self.reads.append(job)
			cond.notify()

	def get_chunk(self, callback, hash):
		hash = bytes(hash)

		cond = self.cond
		with cond:
			env = self.env
		def job():
			try:
				buf = env.get(hash)
				if buf is None:
					raise KeyError(hash)
			except:
				callback(None, exc_info())
			else:
				callback(buf, None)

		with cond:
			self._ensure_started
			self.reads.append(job)
			cond.notify()

	def put_chunk(self, callback, hash, value):
		hash = bytes(hash)

		def op(txn):
			txn.put(hash, value)

		cond = self.cond
		with cond:
			self._ensure_started
			self.writes.value.append(_WriteJob(op, callback))
			if not self.writing:
				cond.notify()

	def del_chunk(self, callback, hash):
		hash = bytes(hash)

		def op(txn):
			txn.delete(hash)

		cond = self.cond
		with cond:
			self._ensure_started
			self.writes.value.append(_WriteJob(op, callback))
			if not self.writing:
				cond.notify()

	def lister(self, agent):
		with self.cond:
			env = self.env
		with env.raw_iterator() as i:
			i.seek_to_first()
			try:
				while True:
					yield i.key()
					i.next()
			except self.IteratorInvalidError:
				pass

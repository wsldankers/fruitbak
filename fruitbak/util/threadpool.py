from threading import Thread
from weakref import ref as weakref
from collections import deque
from os import sched_getaffinity

from .weakheapmap import MaxWeakHeapMap
from .oo import Initializer
from .locking import NCondition

# cancel = remove from queue

class _Mutable:
	__slots__ = 'value',

	def __init__(self, value = None):
		self.value = value

	def __bool__(self):
		return bool(self.value)

class _Result(Initializer):
	__slots__ = 'success', 'result'

	def __init__(self):
		self.success = None
		self.result = None

class _Job:
	failed = False

	def __init__(self, func, args, max_results):
		self.func = func
		self.args = args
		self.max_results = max_results
		self.results = deque()
		self.cond = NCondition()
		try:
			self.next_arg = next(args, None)
		except Exception as e:
			self.next_arg = e

	@property
	def done(self):
		assert self.cond

		return self.next_arg is None

	@property
	def _eligible(self):
		assert self.cond

		# return self._eligibility > 0
		if self.done or self.failed:
			return False

		return len(self.results) < self.max_results

	@property
	def _eligibility(self):
		assert self.cond

		if self.done or self.failed:
			return 0

		return max(0, self.max_results - len(self.results))

	def abort(self):
		cond = self.cond
		with cond:
			self.next_arg = None
			cond.notify()

	def notify(self):
		cond = self.cond
		with cond:
			cond.notify()

	def get_task(self):
		assert self.cond

		cur_arg = self.next_arg
		if cur_arg is None:
			# we're done
			return None

		if isinstance(cur_arg, Exception):
			self.next_arg = None
			raise cur_arg

		try:
			self.next_arg = next(self.args, None)
		except Exception as e:
			self.failed = True
			self.next_arg = e

		return lambda: self.func(*cur_arg)

	def get_queued_task(self, requeue):
		cond = self.cond
		with cond:
			results = self.results
			try:
				task = self.get_task()
			except Exception as e:
				task = None
				self.failed = True
				result = _Result()
				result.success = False
				result.result = e
				results.append(result)
				cond.notify()
			else:
				if task is not None:
					result = _Result()
					results.append(result)

					def queued_task():
						try:
							r = task()
						except Exception as e:
							with cond:
								result.success = False
								result.result = e
								cond.notify()
						else:
							with cond:
								result.success = True
								result.result = r
								cond.notify()

					_eligibility = self._eligibility
					if _eligibility:
						requeue(_eligibility)

					return queued_task

			return None, False


class _Worker(Thread):
	def __init__(self, cond, queue, done, wakeups, *args, **kwargs):
		super().__init__(*args, daemon = True, **kwargs)
		self.cond = cond
		self.queue = queue
		self.done = done
		self.wakeups = wakeups
		self.start()

	def run(self):
		cond = self.cond
		queue = self.queue
		done = self.done

		while True:
			job = None
			with cond:
				while job is None:
					if done:
						return
					try:
						job = queue.popkey()
					except IndexError:
						cond.wait()
					self.wakeups.value += 1

			def requeue(_eligibility):
				with cond:
					queue.move_to_end(job, _eligibility)
					cond.notify()

			task = job.get_queued_task(requeue)

			if task is not None:
				if not task():
					queue.discard(job)

class ThreadPool:
	def __init__(self, max_workers = None, max_results = None):
		if max_workers is None:
			max_workers = len(sched_getaffinity(0))
		if max_results is None:
			max_results = max_workers * 2

		cond = NCondition()
		queue = MaxWeakHeapMap()
		done = _Mutable()
		wakeups = _Mutable(0)

		self.cond = cond
		self.queue = queue
		self.done = done
		self.wakeups = wakeups

		self.max_workers = max_workers
		self.max_results = max_results

		self.workers = [_Worker(cond, queue, done, wakeups) for x in range(max_workers)]

	def __del__(self):
		cond = self.cond
		with cond:
			self.done.value = True
			cond.notify_all()
		self.workers.clear()
		#workers = self.workers
		#for worker in workers:
		#	worker.join()

	def map(self, func, *args, max_results = None):
		if max_results is None:
			max_results = self.max_results
		job = _Job(func, zip(*args), max_results)
		job_cond = job.cond
		results = job.results

		# make sure we always keep the first item for ourselves:
		task = job.get_task()
		if task is None:
			return iter([])

		cond = self.cond
		queue = self.queue
		if not job.done:
			with cond:
				queue[job] = 0
				cond.notify()

		def map(self, task):
			while True:
				yield task()
				task = None
				with job_cond:
					while True:
						if results:
							result = results.popleft()
							_eligibility = job._eligibility
							if _eligibility:
								with cond:
									queue.move_to_end(job, _eligibility)
									cond.notify()
							while result.success is None:
								job_cond.wait()
							if result.success:
								yield result.result
							else:
								raise result.result
						elif job.done:
							queue.discard(job)
							return
						else:
							task = job.get_task()
							if not job._eligible:
								queue.discard(job)
							break

		return map(self, task)

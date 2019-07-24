from threading import Thread, Condition
from weakref import ref as weakref
from collections import deque
from os import sched_getaffinity

from .weakheapmap import MinWeakHeapMap
from .oo import Initializer

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
	def __init__(self, func, args):
		self.func = func
		self.args = args
		self.results = deque()
		self.cond = Condition()
		self.done = _Mutable()
		try:
			self.next_arg = next(args, None)
		except Exception as e:
			self.next_arg = e

	def get_task(self):
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
			self.next_arg = e

		return lambda: self.func(*cur_arg)

class _Worker(Thread):
	def __init__(self, max_results, queue, cond, done, *args, **kwargs):
		super().__init__(*args, daemon = True, **kwargs)
		self.cond = cond
		self.queue = queue
		self.max_results = max_results
		self.done = done
		self.start()

	def run(self):
		cond = self.cond
		queue = self.queue
		done = self.done
		max_results = self.max_results

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

			job_done = job.done
			job_cond = job.cond
			with job_cond:
				results = job.results
				try:
					task = job.get_task()
				except Exception as e:
					task = None
					result = _Result()
					result.success = False
					result.result = e
					results.append(result)
					job_done.value = True
				else:
					if task is None:
						job_done.value = True
					else:
						result = _Result()
						results.append(result)
						num_results = len(results)
						if num_results < max_results:
							with cond:
								queue[job] = num_results
								cond.notify()

				job_cond.notify()

			if task is not None:
				try:
					r = task()
				except Exception as e:
					queue.discard(job)
					result.success = False
					result.result = e
					with job_cond:
						job_done.value = True
						job_cond.notify()
				else:
					result.success = True
					result.result = r
					with job_cond:
						job_cond.notify()

class ThreadPool:
	def __init__(self, max_workers = None, max_results = None):
		if max_workers is None:
			max_workers = len(sched_getaffinity(0))
		if max_results is None:
			max_results = max_workers * 2

		queue = MinWeakHeapMap()
		cond = Condition()
		done = _Mutable()

		self.queue = queue
		self.cond = cond
		self.done = done

		self.max_workers = max_workers
		self.max_results = max_results

		self.workers = [_Worker(max_results, queue, cond, done) for x in range(max_workers)]

	def __del__(self):
		cond = self.cond
		with cond:
			self.done.value = True
			cond.notify_all()
		self.workers.clear()
		#workers = self.workers
		#for worker in workers:
		#	worker.join()

	def map(self, func, *args):
		job = _Job(func, zip(*args))
		job_cond = job.cond
		job_done = job.done
		results = job.results

		# make sure we always keep the first item for ourselves:
		task = job.get_task()
		if task is None:
			return iter([])

		max_results = self.max_results
		queue = self.queue
		cond = self.cond
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
							num_results = len(results)
							if num_results < max_results:
								with cond:
									queue.move_to_end(job, num_results)
									cond.notify()
							while result.success is None:
								job_cond.wait()
							if result.success:
								yield result.result
							else:
								raise result.result
						elif job_done:
							return
						else:
							task = job.get_task()
							if task is None:
								return
							else:
								break

		return map(self, task)

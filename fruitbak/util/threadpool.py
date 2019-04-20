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
	__slots__ = 'status', 'result'

	def __init__(self):
		self.status = None
		self.result = None

class _Job:
	def __init__(self, func, args):
		self.func = func
		self.args = args
		self.results = deque()
		self.cond = Condition()
		self.done = _Mutable()
		self.next_arg = next(args)

	def get_task(self):
		cur_arg = self.next_arg
		if cur_arg is None:
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

			job_cond = job.cond
			with job_cond:
				result = _Result()
				results = job.results
				try:
					task = job.get_task()
				except Exception as e:
					task = None
					result.status = False
					result.result = e
					results.append(result)

				if task is None:
					job.done.value = True
				else:
					results.append(result)

				job_cond.notify()

			if task is not None:
				with cond:
					num_results = len(results)
					if num_results < max_results:
						queue[job] = num_results
						cond.notify()
				try:
					r = task()
				except Exception as e:
					result.status = False
					result.result = e
				else:
					result.status = True
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

		max_results = self.max_results
		queue = self.queue
		cond = self.cond
		with cond:
			queue[job] = 0
			cond.notify()

		job_cond = job.cond
		job_done = job.done
		results = job.results
		def map(self):
			with job_cond:
				while True:
					if results:
						result = results.popleft()
						num_results = len(results)
						if num_results < max_results:
							with cond:
								queue.move_to_end(job, num_results)
								cond.notify()
						while result.status is None:
							job_cond.wait()
						if result.status:
							yield result.result
						else:
							raise result.result
					elif job_done:
						break
					else:
						job_cond.wait()

		return map(self)

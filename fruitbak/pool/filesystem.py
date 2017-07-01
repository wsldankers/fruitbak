from fruitbak.util.clarity import Clarity, initializer
from fruitbak.pool.handler import Storage

from weakref import ref as weakref
from base64 import b64encode
from threading import Thread, Condition
from collections import deque
from warnings import warn
from tempfile import NamedTemporaryFile
from os import mkdir, replace, fsync, unlink

class WorkerThread(Thread):
	def __init__(self, queue, cond):
		super().__init__(daemon = True)
		self.cond = cond
		self.queue = queue
		self.start()

	def run(self):
		cond = self.cond
		queue = self.queue
		while True:
			with cond:
				while not queue:
					cond.wait()
				job = queue.popleft()
			try:
				job()
			except Exception as e:
				warn(e)

class Filesystem(Storage):
	@initializer
	def base_path(self):
		return self.cfg['base_path']

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self.cond = Condition()
		self.queue = deque()
		self.workers = [WorkerThread(self.queue, self.cond) for x in range(32)]

	def hash2path(self, hash):
		b64 = b64encode(hash, b'+_').rstrip(b'=')
		relpath = b'/' + b64[:2] + b'/' + b64[2:]
		base_path = self.base_path
		if isinstance(base_path, str):
			relpath = relpath.decode()
		return base_path + relpath

	def hash2parent(self, hash):
		b64 = b64encode(hash, b'+_').rstrip(b'=')
		relpath = b'/' + b64[:2]
		base_path = self.base_path
		if isinstance(base_path, str):
			relpath = relpath.decode()
		return base_path + relpath

	def get_chunk(self, hash, callback):
		path = self.hash2path(hash)

		def job():
			try:
				with open(path, mode = 'rb', buffering = 0) as f:
					buf = f.read()
				callback(hash, buf)
			except Exception as e:
				callback(hash, e)
		with self.cond:
			self.queue.append(job)
			self.cond.notify()

	def put_chunk(self, hash, value, callback):
		path = self.hash2path(hash)

		def job():
			try:
				with NamedTemporaryFile(dir = self.base_path, buffering = 0, delete = False) as f:
					try:
						offset = f.write(value)
						if offset < len(value):
							m = memoryview(value)
							while offset < len(value):
								offset += f.write(m[offset:])
						f.flush()
						fsync(f.fileno())
						f.close()
						try:
							replace(f.name, path)
						except FileNotFoundError:
							mkdir(self.hash2parent(hash))
							replace(f.name, path)
					except:
						unlink(f.name)
						raise

				callback(hash, None)
			except Exception as e:
				callback(hash, e)
		with self.cond:
			self.queue.append(job)
			self.cond.notify()

	def del_chunk(self, hash, callback):
		path = self.hash2path(hash)

		def job():
			try:
				unlink(path)
				callback(hash, None)
			except FileNotFoundError:
				callback(hash, None)
			except Exception as e:
				callback(hash, e)
		with self.cond:
			self.queue.append(job)
			self.cond.notify()

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.pool.handler import Storage

from weakref import ref as weakref
from base64 import b64encode
from traceback import print_exc
from sys import stderr, exc_info
from concurrent.futures import ThreadPoolExecutor
from tempfile import NamedTemporaryFile
from os import open as os_open, close as os_close, mkdir, replace, fsync, link, unlink, fsdecode, O_DIRECTORY, O_CLOEXEC, O_PATH, O_TMPFILE, O_WRONLY
from pathlib import Path

from time import sleep
from random import random, randrange

# b64order = bytes(b64encode(bytes((i * 4,)), b'+_')[0] for i in range(64)).decode()
# dirs.sort(key = lambda x: b64decode(x+'A=', b'+_'))

# garbage collected variant of os.open()
class sysopen(int):
	closed = False

	def __new__(cls, *args, **kwargs):
		return super().__new__(cls, os_open(*args, **kwargs))

	def __del__(self):
		if not self.closed:
			try:
				os_close(self)
			except:
				pass

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		if not self.closed:
			self.closed = True
			os_close(self)

class Filesystem(Storage):
	@initializer
	def pooldir(self):
		return self.config['pooldir']

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self.executor = ThreadPoolExecutor(max_workers = 32)

	def hash2path(self, hash):
		b64 = b64encode(hash, b'+_').rstrip(b'=')
		return self.pooldir / Path(fsdecode(b64[:2])) / Path(fsdecode(b64[2:]))

	@initializer
	def pooldir_fd(self):
		return sysopen(str(self.pooldir), O_PATH|O_DIRECTORY|O_CLOEXEC)

	def tempfile(self, path):
		def opener(path, flags):
			# gleefully ignores flags
			return os_open(path, O_TMPFILE|O_WRONLY|O_CLOEXEC, dir_fd = self.pooldir_fd)
		return open(path, 'wb', buffering = 0, opener = opener)

	@initializer
	def have_fancy_stuff(self):
		name = b64encode(bytes(randrange(256) for x in range(24)), b'+_')
		try:
			pooldir_fd = self.pooldir_fd
			with self.tempfile('.') as f:
				link("/proc/self/fd/" + str(f.fileno()), name, dst_dir_fd = pooldir_fd)
			unlink(name, dir_fd = pooldir_fd)
			return True
		except:
			return False

	def submit(self, job, *args, **kwargs):
		def windshield():
			try:
				sleep(random() / 10.0)
				job(*args, **kwargs)
			except:
				print_exc(file = stderr)

		self.executor.submit(windshield)

	def get_chunk(self, hash, callback):
		path = self.hash2path(hash)

		def job():
			try:
				with path.open(mode = 'rb', buffering = 0) as f:
					buf = f.read()
				callback(buf, None)
			except:
				callback(None, exc_info())

		self.submit(job)

	def put_chunk(self, hash, value, callback):
		path = self.hash2path(hash)
		path_str = str(path)

		def job():
			try:
				if self.have_fancy_stuff:
					parent = path.parent
					parent_str = str(parent)
					try:
						tmp = self.tempfile(parent_str)
					except FileNotFoundError:
						try:
							parent.mkdir()
						except FileExistsError:
							pass
						tmp = self.tempfile(parent_str)
					with tmp as f:
						offset = f.write(value)
						if offset < len(value):
							m = memoryview(value)
							while offset < len(value):
								offset += f.write(m[offset:])
						f.flush()
						fd = f.fileno()
						fsync(fd)
						try:
							link("/proc/self/fd/" + str(fd), path_str, dst_dir_fd = self.pooldir_fd)
						except FileExistsError:
							pass
				else:
					print("fallback", file = stderr)
					parent = path.parent
					parent_str = str(parent)
					try:
						tmp = NamedTemporaryFile(dir = parent_str, buffering = 0)
					except FileNotFoundError:
						parent.mkdir()
						tmp = NamedTemporaryFile(dir = parent_str, buffering = 0)
					with tmp as f:
						f_path = Path(f.name)
						offset = f.write(value)
						if offset < len(value):
							m = memoryview(value)
							while offset < len(value):
								offset += f.write(m[offset:])
						f.flush()
						fsync(f.fileno())
						try:
							link(f.name, path_str)
						except FileExistsError:
							pass
						except FileNotFoundError:
							try:
								parent.mkdir()
							except FileExistsError:
								pass
							try:
								link(f.name, path_str)
							except FileExistsError:
								pass

				callback(None)
			except:
				callback(exc_info())

		self.submit(job)

	def del_chunk(self, hash, callback):
		path = self.hash2path(hash)

		def job():
			try:
				path.unlink()
			except FileNotFoundError:
				callback(None)
			except:
				callback(exc_info())
			callback(None)

		self.submit(job)

	def lister(self):
		pass

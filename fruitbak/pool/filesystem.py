from fruitbak.util.clarity import Clarity, initializer
from fruitbak.pool.handler import Storage
from fruitbak.pool.agent import PoolReadahead, PoolAction

from hashset import Hashset

from weakref import ref as weakref
from base64 import b64encode, b64decode
from traceback import print_exc
from sys import stderr, exc_info
from concurrent.futures import ThreadPoolExecutor
from tempfile import NamedTemporaryFile
from os import (
	access, fstat,
	open as os_open, close,
	read, write, fsync,
	mkdir, link, unlink,
	listdir,
	fsdecode,
	F_OK,
	O_DIRECTORY, O_CLOEXEC, O_NOCTTY, O_RDONLY, O_WRONLY
)
from errno import EROFS
from pathlib import Path
from re import compile as re

from time import sleep
from random import random, randrange

try:
	from os import O_PATH, O_TMPFILE
except ImportError:
	O_PATH = None
	O_TMPFILE = None

#b64order = bytes(b64encode(bytes((i * 4,)), b'+_')[0] for i in range(64)).decode()
#b64set = set(b64order)
directory_re = re('[A-Za-z0-9+_]{2}')

def my_b64encode(b):
	return b64encode(b, b'+_').rstrip(b'=').decode()

def my_b64decode(s):
	return b64decode(s + '=' * (-len(s) % 4), b'+_')

# garbage collected variant of os.open()
class sysopen(int):
	closed = False

	def __new__(cls, *args, **kwargs):
		return super().__new__(cls, os_open(*args, **kwargs))

	def __del__(self):
		if not self.closed:
			try:
				close(self)
			except:
				pass

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		if not self.closed:
			self.closed = True
			close(self)

class FilesystemListAction(PoolAction):
	directory = None
	cursor = None

	def sync(self):
		super().sync()
		return self.cursor

class FilesystemListahead(PoolReadahead):
	def dequeue(self):
		assert self.pool.locked

		cond = self.cond
		agent = self.agent

		iterator = self.iterator
		if iterator is None:
			self.agent.register_readahead(self)
			return

		directory = next(iterator, None)
		if directory is None:
			self.iterator = None
			self.agent.register_readahead(self)
			return

		action = FilesystemListAction(directory = directory)
		self.queue.append(action)
		pool = self.pool

		def when_done(cursor, exception):
			with cond:
				action.cursor = cursor
				action.exception = exception
				action.done = True
		self.submit(self.filesystem.listdir, when_done, directory)

		agent.register_readahead(self)

class Filesystem(Storage):
	@initializer
	def pooldir(self):
		return self.config['pooldir']

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self.executor = ThreadPoolExecutor(max_workers = 32)

	def hash2path(self, hash):
		b64 = my_b64encode(hash)
		path = Path(fsdecode(b64[:2])) / Path(fsdecode(b64[2:]))
		if self.have_linux_stuff:
			return path
		return self.pooldir / path

	@initializer
	def pooldir_fd(self):
		return sysopen(str(self.pooldir), O_PATH|O_DIRECTORY|O_CLOEXEC|O_NOCTTY)

	@initializer
	def proc_self_fd(self):
		return sysopen("/proc/self/fd", O_PATH|O_DIRECTORY|O_CLOEXEC|O_NOCTTY)

	def tempfile(self, path):
		return sysopen(path, O_TMPFILE|O_WRONLY|O_CLOEXEC|O_NOCTTY, dir_fd = self.pooldir_fd)

	@initializer
	def have_linux_stuff(self):
		if O_PATH is None or O_TMPFILE is None:
			return False
		name = b64encode(bytes(randrange(256) for x in range(24)), b'+_')
		try:
			pooldir_fd = self.pooldir_fd
			with self.tempfile('.') as fd:
				link(str(fd), name, src_dir_fd = self.proc_self_fd, dst_dir_fd = pooldir_fd)
			unlink(name, dir_fd = pooldir_fd)
			return True
		except OSError as e:
			# If the fs is read-only then pooldir_fd was successful and we can only
			# do get_chunk anyway, so that should be enough:
			return e.errno == EROFS
		except:
			return False

	def submit(self, job, *args, **kwargs):
		def windshield():
			try:
				#sleep(random() / 10.0)
				job(*args, **kwargs)
			except:
				print_exc(file = stderr)

		self.executor.submit(windshield)

	def has_chunk(self, callback, hash):
		path = self.hash2path(hash)

		if self.have_linux_stuff:
			pooldir_fd = self.pooldir_fd
			def job():
				try:
					result = access(str(path), F_OK, dir_fd = pooldir_fd)
				except:
					callback(None, exc_info())
				else:
					callback(result, None)
		else:
			def job():
				try:
					result = path.exists()
				except:
					callback(None, exc_info())
				else:
					callback(result, None)

		self.submit(job)

	def get_chunk(self, callback, hash):
		path = self.hash2path(hash)

		if self.have_linux_stuff:
			pooldir_fd = self.pooldir_fd
			def job():
				try:
					results = []
					with sysopen(str(path), O_RDONLY|O_CLOEXEC|O_NOCTTY, dir_fd = pooldir_fd) as fd:
						size = fstat(fd).st_size
						bytes_read = 0
						while bytes_read < size:
							try:
								buf = read(fd, size - bytes_read)
							except InterruptedError:
								pass
							else:
								if not buf:
									break
								bytes_read += len(buf)
								results.append(buf)
					if len(results) == 1:
						buf, = results
					else:
						buf = b''.join(results)

				except:
					callback(None, exc_info())
				else:
					callback(buf, None)
		else:
			def job():
				try:
					with path.open(mode = 'rb', buffering = 0) as f:
						buf = f.read()
				except:
					callback(None, exc_info())
				else:
					callback(buf, None)

		self.submit(job)

	def put_chunk(self, callback, hash, value):
		path = self.hash2path(hash)
		path_str = str(path)
		parent = path.parent
		parent_str = str(parent)

		def job():
			value_len = len(value)
			try:
				if self.have_linux_stuff:
					pooldir_fd = self.pooldir_fd
					if not access(path_str, F_OK, dir_fd = pooldir_fd):
						try:
							tmp = self.tempfile(parent_str)
						except FileNotFoundError:
							try:
								mkdir(parent_str, dir_fd = pooldir_fd)
							except FileExistsError:
								pass
							tmp = self.tempfile(parent_str)
						with tmp as fd:
							while True:
								try:
									offset = write(fd, value)
								except InterruptedError:
									pass
								else:
									break
							if offset < value_len:
								m = memoryview(value)
								while offset < value_len:
									try:
										offset += write(fd, m[offset:])
									except InterruptedError:
										pass
							fsync(fd)
							try:
								link(str(fd), path_str, src_dir_fd = self.proc_self_fd, dst_dir_fd = pooldir_fd)
							except FileExistsError:
								pass
				else:
					if not path.exists():
						try:
							tmp = NamedTemporaryFile(dir = parent_str, buffering = 0)
						except FileNotFoundError:
							parent.mkdir(exist_ok = True)
							tmp = NamedTemporaryFile(dir = parent_str, buffering = 0)
						with tmp as f:
							offset = f.write(value)
							if offset < value_len:
								m = memoryview(value)
								while offset < value_len:
									offset += f.write(m[offset:])
							f.flush()
							fsync(f.fileno())
							try:
								link(f.name, path_str)
							except FileExistsError:
								pass
			except:
				callback(exc_info())
			else:
				callback(None)

		self.submit(job)

	def del_chunk(self, callback, hash):
		path = self.hash2path(hash)

		def job():
			try:
				if self.have_linux_stuff:
					unlink(str(path), dir_fd = self.pooldir_fd)
				else:
					path.unlink()
			except FileNotFoundError:
				callback(None)
			except:
				callback(exc_info())
			else:
				callback(None)

		self.submit(job)

	def lister(self, agent):
		dirs = listdir(str(self.pooldir))
		dirs = list(filter(directory_re.fullmatch, dirs))
		dirs.sort(key = lambda x: b64decode(x+'A=', b'+_'))
		listahead = FilesystemListahead(filesystem = self, agent = agent, iterator = iter(dirs))
		for action in listahead:
			if action.exception:
				raise action.exception[1]
			yield from action.cursor

	def listdir(self, callback, directory):
		hashsize = self.fruitbak.hashsize
		def job():
			try:
				files = listdir(str(self.pooldir / directory))
				hashes = map(lambda x: my_b64decode(directory + x), files)
				del files
				hashbuf = b''.join(hashes)
				del hashes
				cursor = Hashset(hashbuf, hashsize)
			except:
				callback(None, exc_info())
			else:
				callback(cursor, None)

		self.submit(job)

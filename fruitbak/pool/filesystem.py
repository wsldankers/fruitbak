from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.sysopen import sysopen, sysopendir
from fruitbak.pool.handler import Storage
from fruitbak.pool.agent import PoolReadahead, PoolAction
from fruitbak.config import configurable

from hashset import Hashset

from base64 import b64encode, b64decode
from traceback import print_exc
from sys import stderr, exc_info
from concurrent.futures import ThreadPoolExecutor
from threading import get_ident as get_thread_id
from os import (
	access, fstat,
	open as os_open, close,
	read, write, fsync,
	mkdir, link, unlink,
	listdir,
	fsdecode,
	F_OK,
	O_RDONLY, O_WRONLY, O_EXCL, O_CREAT
)

from pathlib import Path
from re import compile as re

from time import sleep
from random import getrandbits

#b64order = bytes(b64encode(bytes((i * 4,)), b'+_')[0] for i in range(64)).decode()
#b64set = set(b64order)
directory_re = re('[A-Za-z0-9+_]{2}')

def my_b64encode(b):
	return b64encode(b, b'+_').rstrip(b'=').decode()

def my_b64decode(s):
	return b64decode(s + '=' * (-len(s) % 4), b'+_')

class FilesystemListAction(PoolAction):
	directory = None
	cursor = None

	def sync(self):
		super().sync()
		return self.cursor

class FilesystemListahead(PoolReadahead):
	def dequeue(self):
		assert self.lock

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
	def __init__(self, **kwargs):
		super().__init__(**kwargs)

	def hash2path(self, hash):
		b64 = my_b64encode(hash)
		return Path(fsdecode(b64[:2])) / fsdecode(b64[2:])

	@configurable
	def pooldir(self):
		return 'pool'

	@pooldir.prepare
	def pooldir(self, value):
		return Path(value)

	@initializer
	def pooldir_fd(self):
		return sysopendir(self.pooldir, dir_fd = self.fruitbak.rootdir_fd)

	def submit(self, job, *args, **kwargs):
		def windshield():
			try:
				#sleep(random() / 10.0)
				job(*args, **kwargs)
			except:
				print_exc(file = stderr)

		self.executor.submit(windshield)

	class NamedTemporaryFile(sysopen):
		def __new__(cls, path, mode = 0o666, *, dir_fd = None):
			name = 'tmp@' + str(get_thread_id()) + '@' + my_b64encode(getrandbits(128).to_bytes(16, 'big'))
			path = Path(path) / name
			self = super().__new__(cls, path, O_WRONLY|O_EXCL|O_CREAT, dir_fd = dir_fd)
			self.path = path
			self.dir_fd = dir_fd
			return self

		@property
		def xpath(self):
			raise RuntimeError("this temporary file is already closed")

		@property
		def xdir_fd(self):
			raise RuntimeError("this temporary file is already closed")

		def close(self):
			path = self.path
			dir_fd = self.dir_fd
			del self.path
			del self.dir_fd
			try:
				unlink(str(path), dir_fd = dir_fd)
			except FileNotFoundError:
				pass

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc_value, traceback):
			self.close()

		def __del__(self):
			try:
				self.close()
			except:
				pass

	def tmpfile(self, path):
		return self.NamedTemporaryFile(path, dir_fd = self.pooldir_fd)

	def has_chunk(self, callback, hash):
		path = self.hash2path(hash)
		pooldir_fd = self.pooldir_fd
		def job():
			try:
				result = access(str(path), F_OK, dir_fd = pooldir_fd)
			except:
				callback(None, exc_info())
			else:
				callback(result, None)
		self.submit(job)

	def get_chunk(self, callback, hash):
		path = self.hash2path(hash)
		pooldir_fd = self.pooldir_fd

		def job():
			try:
				results = []
				with sysopen(path, O_RDONLY, dir_fd = pooldir_fd) as fd:
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

		self.submit(job)

	def put_chunk(self, callback, hash, value):
		path = self.hash2path(hash)
		path_str = str(path)
		parent = path.parent
		tmpfile = self.tmpfile
		pooldir_fd = self.pooldir_fd

		def job():
			value_len = len(value)
			try:
				if not access(path_str, F_OK, dir_fd = pooldir_fd):
					try:
						tmp = tmpfile(parent)
					except FileNotFoundError:
						try:
							mkdir(str(parent), dir_fd = pooldir_fd)
						except FileExistsError:
							pass
						tmp = tmpfile(parent)
					with tmp:
						tmp.write(value)
						fsync(tmp)
						try:
							link(str(tmp.path), path_str, src_dir_fd = pooldir_fd, dst_dir_fd = pooldir_fd)
						except FileExistsError:
							pass
			except:
				callback(exc_info())
			else:
				callback(None)

		self.submit(job)

	def del_chunk(self, callback, hash):
		path = self.hash2path(hash)
		pooldir_fd = self.pooldir_fd

		def job():
			try:
				unlink(str(path), dir_fd = pooldir_fd)
			except FileNotFoundError:
				callback(None)
			except:
				callback(exc_info())
			else:
				callback(None)

		self.submit(job)

	def lister(self, agent):
		dirs = listdir(self.pooldir_fd)
		dirs = list(filter(directory_re.fullmatch, dirs))
		dirs.sort(key = lambda x: b64decode(x+'A=', b'+_'))
		listahead = FilesystemListahead(filesystem = self, agent = agent, iterator = iter(dirs))
		for action in listahead:
			if action.exception:
				raise action.exception[1]
			yield from action.cursor

	def listdir(self, callback, directory):
		hashsize = self.fruitbak.hashsize
		pooldir = self.pooldir
		def job():
			try:
				with sysopendir(directory, dir_fd = self.pooldir_fd) as fd:
					files = listdir(fd)
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

try:
	from os import O_TMPFILE
except ImportError:
	pass
else:
	class LinuxFilesystem(Filesystem):
		@initializer
		def proc_self_fd(self):
			return sysopendir("/proc/self/fd", path_only = True)

		def tmpfile(self, path):
			return sysopen(path, O_TMPFILE|O_WRONLY|O_CLOEXEC|O_NOCTTY, dir_fd = self.pooldir_fd)

		def put_chunk(self, callback, hash, value):
			path = self.hash2path(hash)
			path_str = str(path)
			parent = path.parent
			parent_str = str(parent)
			pooldir_fd = self.pooldir_fd
			proc_self_fd = self.proc_self_fd

			def job():
				value_len = len(value)
				try:
					if not access(path_str, F_OK, dir_fd = pooldir_fd):
						try:
							tmp = self.tmpfile(parent_str)
						except FileNotFoundError:
							try:
								mkdir(parent_str, dir_fd = pooldir_fd)
							except FileExistsError:
								pass
							tmp = self.tmpfile(parent_str)
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
								link(str(fd), path_str, src_dir_fd = proc_self_fd, dst_dir_fd = pooldir_fd)
							except FileExistsError:
								pass
				except:
					callback(exc_info())
				else:
					callback(None)

			self.submit(job)

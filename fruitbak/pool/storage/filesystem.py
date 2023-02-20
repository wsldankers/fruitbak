from fruitbak.util import Initializer, initializer, sysopen, sysopendir, fd, fallback
from fruitbak.pool.storage import Storage
from fruitbak.pool.agent import PoolReadahead, PoolAction
from fruitbak.config import configurable

from hashset import Hashset

from base64 import b64encode, b64decode
from traceback import print_exc
from sys import stderr, exc_info
from threading import get_ident as gettid
from os import getpid, unlink, F_OK, O_RDONLY, O_WRONLY, O_EXCL, O_CREAT, O_NOFOLLOW

from pathlib import Path
from re import compile as regcomp, escape

from time import sleep
from random import choice
from itertools import chain

b64bytes = b'+_'
b64chars = b64bytes.decode()
b64escaped = escape(b64chars)

b64seq = bytes(b64encode(bytes((i << 2,)), b64bytes)[0] for i in range(64)).decode()
#b64set = set(b64seq)

def my_b64encode(b):
	return b64encode(b, b64bytes).rstrip(b'=').decode()

def my_b64decode(s):
	return b64decode(s + '=' * (-len(s) & 3), b64bytes)

class FilesystemListAction(PoolAction):
	directory = None
	cursor = None

	def sync(self):
		super().sync()
		return self.cursor

class FilesystemListahead(PoolReadahead):
	def dequeue(self):
		lock = self.lock
		assert lock

		agent = self.agent

		iterator = self.iterator
		if iterator is None:
			agent.register_readahead(self)
			return

		directory = next(iterator, None)
		if directory is None:
			self.iterator = None
			agent.register_readahead(self)
			return

		action = FilesystemListAction(directory = directory)
		self.queue.append(action)
		pool = self.pool

		def when_done(cursor, exception):
			assert lock
			action.cursor = cursor
			action.exception = exception
			action.done = True
		self.submit(self.filesystem.listdir, when_done, directory)

		agent.register_readahead(self)

class Filesystem(Storage):
	def __init__(self, **kwargs):
		super().__init__(**kwargs)

	@initializer
	def hash_size(self):
		return self.fruitbak.hash_size

	def hash2path(self, hash):
		b64 = my_b64encode(hash)
		return Path(b64[:2]) / b64[2:]

	@initializer
	def is_valid_file_name(self):
		return regcomp('[A-Za-z0-9'+b64escaped+']{%d}' % (
			len(my_b64encode(bytes(self.hash_size))) - 2,
		)).fullmatch

	is_valid_dir_name = regcomp('[A-Za-z0-9'+b64escaped+']{2}').fullmatch

	@configurable
	def pooldir(self):
		return 'pool'

	@pooldir.prepare
	def pooldir(self, value):
		return Path(value)

	@initializer
	def pooldir_fd(self):
		return self.fruitbak.rootdir_fd.sysopendir(self.pooldir)

	@initializer
	def submit(self):
		return self.executor.submit

	class NamedTemporaryFile:
		def __init__(self, path, mode = 0o666, *, dir_fd = None):
			name = ''.join(chain(
				('tmp-', str(getpid()), '-', str(gettid()), '-'),
				(choice(b64seq) for x in range(32)),
			))
			path = Path(name) if path is None else Path(path) / name
			self.path = path
			self.dir_fd = dir_fd
			self.fd = sysopen(path, O_WRONLY|O_EXCL|O_CREAT|O_NOFOLLOW, 0o440, dir_fd = dir_fd)

		@fallback
		def path(self):
			raise RuntimeError("this temporary file is already closed")

		@fallback
		def fd(self):
			raise RuntimeError("this temporary file is already closed")

		@fallback
		def dir_fd(self):
			raise RuntimeError("this temporary file is already closed")

		def close(self):
			if 'fd' not in self.__dict__:
				return
			path = self.path
			fd = self.fd
			dir_fd = self.dir_fd
			del self.fd
			del self.path
			del self.dir_fd
			try:
				fd.close()
			finally:
				try:
					unlink(str(path), dir_fd = dir_fd)
				except FileNotFoundError:
					pass

		def __getattr__(self, name):
			return getattr(self.fd, name)

		def __int__(self):
			return self.fd

		__index__ = __int__

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
				result = pooldir_fd.access(path, F_OK)
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
				with pooldir_fd.sysopen(path, O_RDONLY) as fd:
					buf = fd.read(fd.stat().st_size)
			except:
				callback(None, exc_info())
			else:
				callback(buf, None)

		self.submit(job)

	def put_chunk(self, callback, hash, value):
		path = self.hash2path(hash)
		parent = path.parent
		tmpfile = self.tmpfile
		pooldir_fd = self.pooldir_fd

		def job():
			try:
				if not pooldir_fd.access(path, F_OK):
					try:
						tmp = tmpfile(parent)
					except FileNotFoundError:
						pooldir_fd.mkdir(parent, exist_ok = True)
						tmp = tmpfile(parent)
					with tmp:
						tmp.write(value)
						tmp.sync()
						pooldir_fd.link(tmp.path, path, exist_ok = True)
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
				pooldir_fd.unlink(path, missing_ok = True)
			except:
				callback(exc_info())
			else:
				callback(None)

		self.submit(job)

	def lister(self, agent):
		dirs = self.pooldir_fd.listdir()
		dirs = list(filter(self.is_valid_dir_name, dirs))
		dirs.sort(key = lambda x: b64decode(x+'A=', b64bytes))
		with FilesystemListahead(filesystem = self, agent = agent, iterator = iter(dirs)) as listahead:
			for action in listahead:
				if action.exception:
					raise action.exception[1]
				yield from action.cursor

	def listdir(self, callback, directory):
		hash_size = self.hash_size
		pooldir = self.pooldir
		pooldir_fd = self.pooldir_fd
		is_valid_file_name = self.is_valid_file_name
		def job():
			try:
				with pooldir_fd.sysopendir(directory) as fd:
					hashbuf = b''.join(
						my_b64decode(directory + file)
						for file in fd.listdir()
						if is_valid_file_name(file)
					)
				cursor = Hashset(hashbuf, hash_size)
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
			return self.pooldir_fd.sysopen(path, O_TMPFILE|O_WRONLY, 0o440)

		def put_chunk(self, callback, hash, value):
			path = self.hash2path(hash)
			parent = path.parent
			pooldir_fd = self.pooldir_fd
			proc_self_fd = self.proc_self_fd

			def job():
				try:
					if not pooldir_fd.access(path, F_OK):
						try:
							tmp = self.tmpfile(parent)
						except FileNotFoundError:
							pooldir_fd.mkdir(parent, exist_ok = True)
							tmp = self.tmpfile(parent)
						with tmp as fd:
							fd.write(value)
							fd.sync()
							proc_self_fd.link(str(fd), path, dir_fd = pooldir_fd, exist_ok = True)
				except:
					callback(exc_info())
				else:
					callback(None)

			self.submit(job)

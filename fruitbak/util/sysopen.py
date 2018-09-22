from os import (
	open as os_open,
	close as os_close,
	read as os_read,
	write as os_write,
	scandir as os_scandir,
	listdir,
	mkdir,
	stat,
	fsencode,
	fsdecode,
	O_DIRECTORY,
	O_CLOEXEC,
	O_RDONLY,
	O_NOCTTY,
	O_NOFOLLOW,
)
from os.path import samestat
from stat import S_ISDIR, S_ISREG, S_ISLNK
from pathlib import PurePath

from fruitbak.util import Clarity, initializer

try:
	from os import O_PATH
except ImportError:
	# that's ok, O_PATH is just advisory
	O_PATH = 0

try:
	from os import O_LARGEFILE
except ImportError:
	# that's ok, O_LARGEFILE is just advisory
	O_LARGEFILE = 0

def sysopendir(path, dir_fd = None, mode = 0o777, path_only = None, follow_symlinks = True, create_ok = False):
	flags = O_DIRECTORY|O_RDONLY
	if isinstance(path, PurePath):
		path = str(path)
	if path_only:
		flags |= O_PATH
	if create_ok:
		try:
			return sysopen(path, flags, dir_fd = dir_fd, follow_symlinks = follow_symlinks)
		except FileNotFoundError:
			try:
				mkdir(path, mode, dir_fd = dir_fd)
			except FileExistsError:
				pass
	return sysopen(path, flags, dir_fd = dir_fd, follow_symlinks = follow_symlinks)

def opener(**kwargs):
	def opener(path, flags):
		return os_open(path, flags|O_CLOEXEC|O_NOCTTY, **kwargs)
	return opener

class DirEntry(Clarity):
	@property
	def path(self):
		return self.name

	def __fspath__(self):
		return self.name

	def __str__(self):
		return fsdecode(self.name)

	def __bytes__(self):
		return fsencode(self.name)

	_stat_exception = None

	@initializer
	def _stat_result(self):
		e = self._stat_exception
		if e is None:
			try:
				return stat(self.name, dir_fd = self.dir_fd, follow_symlinks = True)
			except Exception as e:
				self._stat_exception = e
		raise e

	_lstat_exception = None

	@initializer
	def _lstat_result(self):
		e = self._lstat_exception
		if e is None:
			try:
				return stat(self.name, dir_fd = self.dir_fd, follow_symlinks = False)
			except Exception as e:
				self._lstat_exception = e
		raise e

	def stat(self, *, follow_symlinks = True):
		return self._stat_result if follow_symlinks else self._lstat_result

	def inode(self):
		return self.stat(follow_symlinks = False).st_ino

	def is_dir(self, follow_symlinks = True):
		try:
			st = self.stat(follow_symlinks = follow_symlinks)
		except FileNotFoundError:
			return False
		else:
			return S_ISDIR(st.st_mode)

	def is_file(self, *, follow_symlinks = True):
		try:
			st = self.stat(follow_symlinks = follow_symlinks)
		except FileNotFoundError:
			return False
		else:
			return S_ISREG(st.st_mode)

	def is_symlink(self):
		try:
			st = self.stat(follow_symlinks = False)
		except FileNotFoundError:
			return False
		else:
			return S_ISLNK(st.st_mode)

class sysopen(int):
	"""Wrapper for os.open() with some amenities such as garbage collection,
	context methods, utility methods for reading and writing reliably"""
	closed = False

	def __new__(cls, path, flags, mode = 0o666, follow_symlinks = True, controlling_tty = False, close_on_exec = True, **kwargs):
		flags |= O_LARGEFILE
		if not follow_symlinks:
			flags |= O_NOFOLLOW
		if not controlling_tty:
			flags |= O_NOCTTY
		if close_on_exec:
			flags |= O_CLOEXEC
		if isinstance(path, PurePath):
			path = str(path)
		fd = os_open(path, flags, mode, **kwargs)
		try:
			return super().__new__(cls, fd)
		except:
			close(fd)
			raise

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

	def close(self):
		if not self.closed:
			self.closed = True
			os_close(self)

	def read(self, size):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		results = []
		while size > 0:
			buf = os_read(self, size)
			if not buf:
				break
			size -= len(buf)
			results.append(buf)

		if len(results) == 1:
			return results[0]
		else:
			return b''.join(results)

	def write(self, buffer):
		buffer_len = len(buffer)
		offset = os_write(self, buffer)
		if offset < buffer_len:
			if not isinstance(buffer, memoryview):
				buffer = memoryview(buffer)
			while offset < buffer_len:
				offset += os_write(self, buffer[offset:])

	def scandir(self):
		try:
			yield from os_scandir(self)
		except TypeError:
			for name in listdir(self):
				yield DirEntry(name = name, dir_fd = self)

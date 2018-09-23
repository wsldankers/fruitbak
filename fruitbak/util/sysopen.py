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

def unpath(path):
	if isinstance(path, PurePath):
		return str(path)
	else:
		return path

def is_bytes_like(obj):
	try:
		memoryview(obj)
	except TypeError:
		return False
	else:
		return True

def sysopendir(path, dir_fd = None, mode = 0o777, path_only = None, follow_symlinks = True, create_ok = False):
	flags = O_DIRECTORY|O_RDONLY
	path = unpath(path)
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

	def __new__(cls, path, flags, mode = 0o666, follow_symlinks = True, controlling_tty = False, inheritable = True, **kwargs):
		flags |= O_LARGEFILE
		if not follow_symlinks:
			flags |= O_NOFOLLOW
		if not controlling_tty:
			flags |= O_NOCTTY
		if inheritable:
			flags |= O_CLOEXEC
		fd = os_open(unpath(path), flags, mode, **kwargs)
		try:
			return super().__new__(cls, fd)
		except:
			os_close(fd)
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

	def listdir(self):
		return listdir(self)

	def fstat(self, **kwargs):
		return os_stat(self, dir_fd = None, **kwargs)

	def sysdup(self):
		fd = dup(self)
		try:
			return super(sysopen, sysopen).__new__(sysopen, fd)
		except:
			os_close(fd)
			raise


	def truncate(self, arg, *args, **kwargs):
		if args:
			# arg is a path, args contains length
			if isinstance(arg, int):
				raise RuntimeError("to truncate the fd itself, omit the parameter")
			return os_truncate(unpath(arg), *args, dir_fd = self, **kwargs)
		else:
			# arg is the length
			return os_truncate(self, arg, dir_fd = None, **kwargs)

	def stat(self, *args, **kwargs):
		if args:
			path = args.pop[0]
			if isinstance(path, int):
				raise RuntimeError("to stat the fd itself, use fstat")
			return os_stat(unpath(path), *args, dir_fd = self, **kwargs)
		else:
			return os_stat(self, dir_fd = None, **kwargs)

	def utime(self, *args, **kwargs):
		if args:
			arg = args[0]
			if isinstance(arg, int):
				raise RuntimeError("to utime the fd itself, omit the parameter")
			if isinstance(arg, str) or isinstance(arg, PurePath) or is_bytes_like(arg):
				args.pop(0)
				return os_utime(unpath(arg), *args, dir_fd = self, **kwargs)
		return os_utime(self, *args, dir_fd = None, **kwargs)


	def open(self, path, flags, *args, **kwargs):
		os_open(unpath(path), flags, *args, dir_fd = self, **kwargs)

	def sysopen(self, path, *args, **kwargs):
		return sysopen(path, *args, dir_fd = self, **kwargs)

	def sysdiropen(self, path, **kwargs):
		return sysdiropen(path, dir_fd = self, **kwargs)

	def mkdir(self, path, *args, exist_ok = False, **kwargs):
		try:
			return os_mkdir(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def mkpipe(self, path, *args, exist_ok = False, **kwargs):
		try:
			return os_mkpipe(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def mknod(self, path, *args, exist_ok = False, **kwargs):
		try:
			return os_mknod(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def symlink(self, src, *args, exist_ok = False, **kwargs):
		try:
			return os_symlink(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def unlink(self, path, *, missing_ok = False, **kwargs):
		try:
			return os_unlink(unpath(path), dir_fd = self, **kwargs)
		except FileNotFoundError:
			if not missing_ok:
				raise

	def remove(self, path, *, missing_ok = False, **kwargs):
		try:
			return os_remove(unpath(path), dir_fd = self, **kwargs)
		except FileNotFoundError:
			if not missing_ok:
				raise

	def rmdir(self, path, **kwargs):
		try:
			return os_rmdir(unpath(path), dir_fd = self, **kwargs)
		except FileNotFoundError:
			if not missing_ok:
				raise

	def rename(self, src, dst, *, dir_fd = None, **kwargs):
		if dir_fd is None:
			dir_fd = self
		return os_rename(unpath(src), unpath(dst), src_dir_fd = self, dst_dir_fd = dir_fd, **kwargs)

	def replace(self, src, dst, *, dir_fd = None, **kwargs):
		if dir_fd is None:
			dir_fd = self
		return os_replace(unpath(src), unpath(dst), src_dir_fd = self, dst_dir_fd = dir_fd, **kwargs)

	def link(self, src, dst, *, dir_fd = None, **kwargs):
		if dir_fd is None:
			dir_fd = self
		return os_link(unpath(src), unpath(dst), src_dir_fd = self, dst_dir_fd = dir_fd, **kwargs)

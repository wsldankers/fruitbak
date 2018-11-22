"""Garbage collected POSIX file descriptors that provide POSIX
functionality as methods::

	dir_fd = opendir("/tmp")
	fd = dir_fd.open("temprace", O_CREAT, 0666)
	fd.close()
	dir_fd.unlink("temprace")

In this example, dir_fd will be closed automatically when garbage
collected (usually when it goes out of scope)."""

from os import (
	access as os_access,
	chmod as os_chmod,
	chown as os_chown,
	close as os_close,
	device_encoding as os_device_encoding,
	dup as os_dup,
	dup2 as os_dup2,
	fdatasync as os_fdatasync,
	fdopen as os_fdopen,
	fsync as os_fsync,
	link as os_link,
	listdir as os_listdir,
	mkdir as os_mkdir,
	mkfifo as os_mkfifo,
	mknod as os_mknod,
	open as os_open,
	pathconf as os_pathconf,
	read as os_read,
	readlink as os_readlink,
	remove as os_remove,
	rename as os_rename,
	replace as os_replace,
	rmdir as os_rmdir,
	scandir as os_scandir,
	stat as os_stat,
	statvfs as os_statvfs,
	symlink as os_symlink,
	truncate as os_truncate,
	unlink as os_unlink,
	utime as os_utime,
	write as os_write,
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

from fruitbak.util import Clarity, initializer, flexiblemethod, is_byteslike

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

def unpath(obj):
	"""Convert Path-like objects to str; pass through other objects
	unmodified.

	:param obj: The object to (potentially) convert"""
	:type obj: str or bytes or PurePath
	:return: either a stringified version of the Path or the unmodified parameter
	:rtype: str or bytes"""
	if isinstance(obj, PurePath):
		return str(obj)
	else:
		return obj

def opener(mode = 0o666, **kwargs):
	"""Create an opener suitable for use with the builtin Python `open`
	function's `opener` parameter. See the documentation for Python's `open`
	for details about what this parameter is used for.

	This function returns an opener that will call `os.open` with the supplied
	mode and other arguments, as well as the `flags` arguments that Python's
	`open` will provide (bitwise or'd with ``O_CLOEXEC|O_NOCTTY``).

	:param int mode: Integer modes to pass to `os.open`.
	:param dict kwargs: Additional arguments that will be passed to `os.open`.
	:return: A function that is a suitable argument to Python's `open`."""
	def opener(path, flags):
		"""This opener was created by `fruitbak.util.fd.opener()`.

		:param path: The path to open (as passed to Python's `open` function).
		:type path: str or bytes or Path
		:return: A plain file descriptor.
		:rtype: int"""
		return os_open(unpath(path), flags|O_CLOEXEC|O_NOCTTY, mode = mode, **kwargs)
	return opener

class DirEntry(Clarity):
	"""DirEntry(*, name, dir_fd)

	Older versions of `os.scandir` do not accept a file descriptor as an
	argument. The `DirEntry` class is part of a polyfill for that case. It
	provides the same functionality as the built-in DirEntry type, though it
	lacks support for one optimization that some filesystems provide (the
	`d_type` field of the C library `readdir(3)` function).

	:param name: The (bare) name of this entry.
	:type name: str or bytes
	:param int dir_fd: the directory fd relative to which `stat` operations
		on `name` will be performed."""

	@property
	def path(self):
		"""Normally the full name, these objects do not have parent information
		because they are based off a listing of a file descriptor. Therefore this
		entry is the same as the name attribute, i.e., the bare name without any
		parent directory names prefixed to it.

		:type: str or bytes"""
		return self.name

	def __fspath__(self):
		"""Called by `Path` objects when used as an argument for the constructor.
		Because this constructor would otherwise call `str()` on us and by doing
		that possibly unnecessarily convert the filename from `bytes` to `str`,
		we make sure it gets the original.

		:return: The name.
		:rtype: str or bytes"""
		return self.name

	def __str__(self):
		"""Convert the name to a string using Python's current filesystem encoding.

		:return: The name as a string.
		:rtype: str"""

		return fsdecode(self.name)

	def __bytes__(self):
		"""Convert the name to a bytes object using Python's current filesystem encoding.

		:return: The name as a bytes object.
		:rtype: bytes"""
		return fsencode(self.name)

	_stat_exception = None
	"""If the last `stat` call raised an error, we store it here so we can keep raising
	it instead of retrying it every time.

	:type: Exception or None"""

	@initializer
	def _stat_result(self):
		"""The result of the last `stat` we performed on this directory entry.
		Initialized by actually performing the `stat`. If this `stat` ever fails, we
		cache the exception instead (and keep raising it henceafter).

		:type: os.stat_result"""
		e = self._stat_exception
		if e is None:
			try:
				return os_stat(self.name, dir_fd = self.dir_fd, follow_symlinks = True)
			except Exception as e:
				self._stat_exception = e
		raise e

	_lstat_exception = None
	"""If the last `lstat` call raised an error, we store it here so we can keep raising
	it instead of retrying it every time.

	:type: Exception or None"""

	@initializer
	def _lstat_result(self):
		"""The result of the last `lstat` we performed on this directory entry.
		Initialized by actually performing the `lstat`. If this `lstat` ever fails, we
		cache the exception instead (and keep raising it henceafter).

		:type: os.stat_result"""
		e = self._lstat_exception
		if e is None:
			try:
				return os_stat(self.name, dir_fd = self.dir_fd, follow_symlinks = False)
			except Exception as e:
				self._lstat_exception = e
		raise e

	def stat(self, *, follow_symlinks = True):
		"""Perform a `stat` on this directory entry. This is only tried once;
		if it fails, it will keep raising the same error.

		;param: boolean follow_symlinks: Whether to follow symlinks when
			doing the `stat`.
		:return: The result of `os.stat`.
		:rtype: os.stat_result"""
		return self._stat_result if follow_symlinks else self._lstat_result

	def inode(self, follow_symlinks = False):
		return self.stat(follow_symlinks = follow_symlinks).st_ino

	def is_dir(self, *, follow_symlinks = True):
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

class fd(int):
	"""Wrapper for os.open() with some amenities such as garbage collection,
	context methods, utility methods for reading and writing reliably"""
	closed = False

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

	@flexiblemethod
	def sysopen(self, path, *args, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		return type(self).sysopen(path, *args, dir_fd = self, **kwargs)

	@flexiblemethod
	def sysopendir(self, path, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		return type(self).sysopendir(path, dir_fd = self, **kwargs)

	@sysopen.classmethod
	def sysopen(cls, path, flags, mode = 0o666, large_file = True, follow_symlinks = True, controlling_tty = False, inheritable = True, **kwargs):
		flags |= O_LARGEFILE
		if not follow_symlinks:
			flags |= O_NOFOLLOW
		if not controlling_tty:
			flags |= O_NOCTTY
		if inheritable:
			flags |= O_CLOEXEC
		unmanaged_fd = os_open(unpath(path), flags, mode, **kwargs)
		try:
			return cls(unmanaged_fd)
		except:
			os_close(unmanaged_fd)
			raise

	@sysopendir.classmethod
	def sysopendir(cls, path, dir_fd = None, mode = 0o777, path_only = None, follow_symlinks = True, create_ok = False):
		flags = O_DIRECTORY|O_RDONLY
		path = unpath(path)
		if path_only:
			flags |= O_PATH
		if create_ok:
			try:
				return cls.sysopen(path, flags, dir_fd = dir_fd, large_file = False, follow_symlinks = follow_symlinks)
			except FileNotFoundError:
				try:
					os_mkdir(path, mode, dir_fd = dir_fd)
				except FileExistsError:
					pass
		return cls.sysopen(path, flags, dir_fd = dir_fd, large_file = False, follow_symlinks = follow_symlinks)

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
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		buffer_len = len(buffer)
		offset = os_write(self, buffer)
		if offset < buffer_len:
			if not isinstance(buffer, memoryview):
				buffer = memoryview(buffer)
			while offset < buffer_len:
				offset += os_write(self, buffer[offset:])

	def scandir(self):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		try:
			yield from os_scandir(self)
		except TypeError:
			for name in os_listdir(self):
				yield DirEntry(name = name, dir_fd = self)

	def listdir(self):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		return os_listdir(self)

	def dup(self):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		unmanaged_fd = os_dup(self)
		try:
			return fd(unmanaged_fd)
		except:
			os_close(unmanaged_fd)
			raise

	def dup2(self, fd, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		os_dup2(self, fd, **kwargs)
		return fd

	def fdopen(self, *args, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		if args and isinstance(args[0], int):
			raise RuntimeError("the first argument of fdopen is already taken care of")
		fp = os_fdopen(self, *args, **kwargs)
		self.closed = True
		return fp

	def device_encoding(self):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		return os_device_encoding(self)

	def chmod(self, *args, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		if args < 3:
			return os_chmod(self, *args, dir_fd = None, **kwargs)
		else:
			path = args[0]
			if isinstance(path, int):
				raise RuntimeError("to chmod the fd itself, omit the fd parameter")
			return os_chmod(unpath(path), *args[1:], dir_fd = self, **kwargs)

	def chown(self, *args, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		if args < 3:
			return os_chown(self, *args, dir_fd = None, **kwargs)
		else:
			path = args[0]
			if isinstance(path, int):
				raise RuntimeError("to chown the fd itself, omit the fd parameter")
			return os_chown(unpath(path), *args[1:], dir_fd = self, **kwargs)

	def sync(self):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		return os_fsync(self)

	def datasync(self):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		return os_fdatasync(self)

	def pathconf(self, name):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		return os_pathconf(self, name)

	def statvfs(self):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		return os_statvfs(self)

	def truncate(self, arg, *args, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		if args:
			# arg is a path, args contains length
			if isinstance(arg, int):
				raise RuntimeError("to truncate the fd itself, omit the parameter")
			return os_truncate(unpath(arg), *args, dir_fd = self, **kwargs)
		else:
			# arg is the length
			return os_truncate(self, arg, dir_fd = None, **kwargs)

	def access(self, path, *args, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		return os_access(unpath(path), *args, dir_fd = self, **kwargs)

	def stat(self, *args, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		if args:
			path = args[0]
			if isinstance(path, int):
				raise RuntimeError("to stat the fd itself, use fstat")
			return os_stat(unpath(path), *args[1:], dir_fd = self, **kwargs)
		else:
			return os_stat(self, dir_fd = None, **kwargs)

	def readlink(self, path, *args, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		return os_readlink(unpath(path), *args, dir_fd = self, **kwargs)

	def utime(self, *args, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		if args:
			arg = args[0]
			if isinstance(arg, int):
				raise RuntimeError("to utime the fd itself, omit the parameter")
			if isinstance(arg, str) or isinstance(arg, PurePath) or is_byteslike(arg):
				return os_utime(unpath(arg), *args[1:], dir_fd = self, **kwargs)
		return os_utime(self, *args, dir_fd = None, **kwargs)

	@property
	def opener(self):
		def opener(path, flags):
			if self.closed:
				raise ValueError("I/O operation on closed file.")
			return os_open(path, flags|O_CLOEXEC|O_NOCTTY, mode = 0o666, dir_fd = self)
		return opener

	def open(self, path, flags, *args, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		os_open(unpath(path), flags, *args, dir_fd = self, **kwargs)

	def mkdir(self, path, *args, exist_ok = False, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		try:
			return os_mkdir(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def mkfifo(self, path, *args, exist_ok = False, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		try:
			return os_mkfifo(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def mknod(self, path, *args, exist_ok = False, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		try:
			return os_mknod(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def symlink(self, src, *args, exist_ok = False, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		try:
			return os_symlink(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def unlink(self, path, *, missing_ok = False, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		try:
			return os_unlink(unpath(path), dir_fd = self, **kwargs)
		except FileNotFoundError:
			if not missing_ok:
				raise

	def remove(self, path, *, missing_ok = False, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		try:
			return os_remove(unpath(path), dir_fd = self, **kwargs)
		except FileNotFoundError:
			if not missing_ok:
				raise

	def rmdir(self, path, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		try:
			return os_rmdir(unpath(path), dir_fd = self, **kwargs)
		except FileNotFoundError:
			if not missing_ok:
				raise

	def rename(self, src, dst, *, dir_fd = None, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		if dir_fd is None:
			dir_fd = self
		return os_rename(unpath(src), unpath(dst), src_dir_fd = self, dst_dir_fd = dir_fd, **kwargs)

	def replace(self, src, dst, *, dir_fd = None, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		if dir_fd is None:
			dir_fd = self
		return os_replace(unpath(src), unpath(dst), src_dir_fd = self, dst_dir_fd = dir_fd, **kwargs)

	def link(self, src, dst, *, dir_fd = None, exist_ok = False, **kwargs):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		if dir_fd is None:
			dir_fd = self
		try:
			return os_link(unpath(src), unpath(dst), src_dir_fd = self, dst_dir_fd = dir_fd, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

sysopen = fd.sysopen
sysopendir = fd.sysopendir

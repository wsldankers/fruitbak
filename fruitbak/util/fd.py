"""Garbage collected POSIX file descriptors that provide POSIX
functionality as methods::

	dir_fd = opendir("/tmp")
	fd = dir_fd.open("temprace", O_CREAT, 0666)
	fd.truncate(4096)
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
from fcntl import flock as fcntl_flock, LOCK_EX, LOCK_SH, LOCK_UN, LOCK_NB

from fruitbak.util.oo import Initializer, initializer, flexiblemethod
from fruitbak.util.strbytes import is_byteslike

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

	:param obj: The object to (potentially) convert
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
	`mode` and other arguments, as well as the `flags` arguments that Python's
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

class flock:
	"""A class that wraps flock() and releases the lock at the end of its
	lifetime. Can also be used as a context manager and indeed this is the
	recommended way to use it.

	See `fcntl.flock` in the Python standard library for possible values for
	`operation`. For your convenience the `fd` module exports the LOCK_*
	symbols as well.

	The mere act of creating the object acquires the lock on the file
	descriptor.

	:param fd int: The file descriptor to lock.
	:param operation int: The type of operation. Defaults to LOCK_EX."""

	_locked = False

	def __init__(self, fd, operation = LOCK_EX):
		self._fd = fd
		fcntl_flock(fd, operation)
		self._locked = True

	def unlock(self):
		"""Unlock the file descriptor. Unlikely to fail unless the file descriptor
		was closed."""

		if self._locked:
			self._locked = False
			fcntl_flock(self._fd, LOCK_UN)

	@property
	def locked(self):
		"""Whether the `fcntl` object thinks the file descriptor is still
		locked."""

		return self._locked

	def __enter__(self):
		"""Context manager that simply returns the flock itself.

		:return: The fcntl object itself.
		:rtype: fruitbak.util.fd.fcntl"""

		return self

	def __exit__(self, exc_type, exc_value, traceback):
		"""Context exit method that simply unlocks the file descriptor."""

		self.unlock()

	def __del__(self):
		self.unlock()

class DirEntry(Initializer):
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
			if '_lstat_result' in vars(self) and not S_ISLNK(self._lstat_result):
				return self._lstat_result
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

		:param bool follow_symlinks: Follow symlinks when doing the `stat`.
		:return: The result of `os.stat`.
		:rtype: os.stat_result"""
		return self._stat_result if follow_symlinks else self._lstat_result

	def inode(self, *, follow_symlinks = False):
		"""Determine the inode number of this entry (or – optionally – in the
		case of a symlink, the inode of whatever the symlink points at).

		:param bool follow_symlinks: Return the inode of the entry
			this symlink points at instead of the entry itself.
		:return: The inode number.
		:rtype: int"""

		return self.stat(follow_symlinks = follow_symlinks).st_ino

	def is_dir(self, *, follow_symlinks = True):
		"""Determine whether this entry (or – optionally – in the case of a
		symlink, whatever the symlink points at) is a directory.

		If the entry does not exist, returns False instead of raising an
		exception.

		:param bool follow_symlinks: Test the entry this symlink points at instead
			of the entry itself.
		:return: Whether this entry exists and is a directory.
		:rtype: bool"""

		try:
			st = self.stat(follow_symlinks = follow_symlinks)
		except FileNotFoundError:
			return False
		else:
			return S_ISDIR(st.st_mode)

	def is_file(self, *, follow_symlinks = True):
		"""Determine whether this entry (or – optionally – in the case of a
		symlink, whatever the symlink points at) is a regular file.

		If the entry does not exist, returns False instead of raising an
		exception.

		:param bool follow_symlinks: Test the entry this symlink points at instead
			of the entry itself.
		:return: Whether this entry exists and is a regular file.
		:rtype: bool"""

		try:
			st = self.stat(follow_symlinks = follow_symlinks)
		except FileNotFoundError:
			return False
		else:
			return S_ISREG(st.st_mode)

	def is_symlink(self):
		"""Determine whether this entry is a symbolic link.

		If the entry does not exist, returns False instead of raising an
		exception.

		:return: Whether this entry exists and is a symbolic link.
		:rtype: bool"""

		try:
			st = self.stat(follow_symlinks = False)
		except FileNotFoundError:
			return False
		else:
			return S_ISLNK(st.st_mode)

class fd(int):
	"""Wrapper for file descriptors with some amenities such as garbage
	collection, context support, utility methods for reading and writing
	reliably. Provides methods for most fd-related functions in Python's
	`os` module.

	Create objects of this class using the `sysopen` function or class
	method, or by directly instantiating it with an unmanaged integer
	file descriptor as a parameter. Examples::

		my_motd_fd = fd.sysopen('/etc/motd', os.O_RDONLY)

		my_sock_fd = fd(socket.socket())

	Most methods are thin wrappers around functions in Python's `os` module."""

	closed = False

	def __del__(self):
		if not self.closed:
			try:
				os_close(self)
			except:
				pass

	def __enter__(self):
		"""Context manager that simply returns the fd itself.

		:return: The fd itself.
		:rtype: fruitbak.util.fd.fd"""

		return self

	def __exit__(self, exc_type, exc_value, traceback):
		"""Context exit method that simply closes the file descriptor."""
		self.close()

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
	def sysopen(cls, path, flags, mode = 0o666, large_file = True, follow_symlinks = True, controlling_tty = False, inheritable = False, **kwargs):
		"""Open a file using `os.open`. This method can be used both as a
		class method and an instance method. If it is used as an instance
		method, the instance is assumed to be a file descriptor for a directory
		and passed to the `os.open` call in the dir_fd parameter.

		All additional parameters are passed to `os.open`.

		:param path: The filesystem path to open.
		:type path: str or bytes or Path
		:param int flags: Flags for `os.open`
		:param int mode: Permission bits for files created by `os.open`.
		:param bool large_file: Add `O_LARGEFILE` to `flags`.
		:param bool follow_symlinks: Do not add `O_NOFOLLOW` to `flags`.
		:param bool controlling_tty: Do not add `O_NOCTTY` to `flags`.
		:param bool inheritable: Do not add `O_CLOEXEC` to `flags`.
		:return: The fd for the opened file.
		:rtype: fruitbak.util.fd.fd"""

		if large_file:
			flags |= O_LARGEFILE
		if not follow_symlinks:
			flags |= O_NOFOLLOW
		if not controlling_tty:
			flags |= O_NOCTTY
		if not inheritable:
			flags |= O_CLOEXEC
		unmanaged_fd = os_open(unpath(path), flags, mode, **kwargs)
		try:
			return cls(unmanaged_fd)
		except:
			os_close(unmanaged_fd)
			raise

	@sysopendir.classmethod
	def sysopendir(cls, path, dir_fd = None, mode = 0o777, path_only = False, create_ok = False, **kwargs):
		"""Open a directory using `os.open`. This method can be used both as a
		class method and an instance method. If it is used as an instance
		method, the instance is assumed to be a file descriptor for a directory
		and passed to the `os.open` call in the dir_fd parameter.

		All additional parameters are passed to `sysopen`.

		:param path: The filesystem path to open.
		:type path: str or bytes or Path
		:param int mode: Permission bits for directories created by this method.
		:param bool path_only: Add `O_PATH` to `flags`.
		:param bool create_ok: Create the directory if it does not exist.
		:return: The fd for the opened directory.
		:rtype: fruitbak.util.fd.fd"""

		flags = O_DIRECTORY|O_RDONLY
		path = unpath(path)
		if path_only:
			flags |= O_PATH
		if create_ok:
			try:
				return cls.sysopen(path, flags, dir_fd = dir_fd, large_file = False, **kwargs)
			except FileNotFoundError:
				try:
					os_mkdir(path, mode, dir_fd = dir_fd)
				except FileExistsError:
					pass
		return cls.sysopen(path, flags, dir_fd = dir_fd, large_file = False, **kwargs)

	def close(self):
		"""Close the file descriptor using `os.close`. Using this method ensures
		that the file descriptor number is only closed once."""

		if not self.closed:
			self.closed = True
			os_close(self)

	def read(self, size):
		"""Read bytes from the file descriptor, using as many `os.read` operations
		as necessary to read `size` bytes. The returned bytes object may be shorter
		than `size` if EOF is reached.

		:param int size: The number of bytes to read.
		:return: The bytes read from the file descriptor.
		:rtype: bytes"""

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
		"""Write a buffer to the file descriptor, using as many `os.write`
		operations as necessary to get the whole buffer out.

		:param bytes buffer: The bytes to write to this file descriptor."""

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
		"""Assuming that this file descriptor refers to a directory, iterate over
		its entries and yield each as an `os.DirEntry` object. For older versions
		of Python that do not support passing an fd as the argument to
		`os.scandir`, `fd.DirEntry` objects are returned instead.

		:return: An iterator of either `os.DirEntry` or `fd.DirEntry` objects for
			each file in this directory.
		:rtype: iter(os.DirEntry or fd.DirEntry)"""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		try:
			yield from os_scandir(self)
		except TypeError:
			for name in os_listdir(self):
				yield DirEntry(name = name, dir_fd = self)

	def listdir(self):
		"""Assuming that this file descriptor refers to a directory, return a list
		of the names of the entries in it. The list is in arbitrary order and does
		not include ``.`` or ``..``.

		:return: A list with the names of the directory entries.
		:rtype: list(str)"""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		return os_listdir(self)

	def dup(self):
		"""Duplicate the file descriptor and return it as a managed
		file descriptor.

		:return: A new file descriptor referring to the same file,
			directory, socket, etc.
		:rtype: fruitbak.fd.fd"""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		unmanaged_fd = os_dup(self)
		try:
			return fd(unmanaged_fd)
		except:
			os_close(unmanaged_fd)
			raise

	def dup2(self, fd, **kwargs):
		"""Duplicate the file descriptor to the given file descriptor number.
		The duplicated file descriptor is *not* wrapped as a managed file
		descriptor.

		.. note:: If the destination file descriptor already exists and is open,
			it is closed first. However, this is done on the operating system level.
			If it was wrapped in an `fd` object, that object will still think it
			manages it.

		Any additional parameters are passed to `os.dup2`.

		:param int fd: The destination file descriptor.
		:return: The destination file descriptor.
		:rtype: int

		"""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		os_dup2(self, fd, **kwargs)
		return fd

	def fdopen(self, *args, **kwargs):
		"""Return a Python file handle that uses this file descriptor for its I/O
		operations. After this call the file descriptor is considered closed
		because the Python file handle has taken over ownership and will close it
		when it is itself closed.

		Any additional parameters are passed to `os.fdopen` (which in turn passes
		them to Python's built-in `open`).

		:return: The Python file handle.
		:rtype: IOBase"""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		if args and isinstance(args[0], int):
			raise RuntimeError("the first argument of fdopen is already taken care of")
		fp = os_fdopen(self, *args, **kwargs)
		self.closed = True
		return fp

	def device_encoding(self):
		"""Return a string describing the encoding of the device associated with
		this file descriptor if it is connected to a terminal; else return None.

		:return: The encoding of the device.
		:rtype: str or None"""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		return os_device_encoding(self)

	def chmod(self, *args, **kwargs):
		"""chmod([path,] mode, *args, **kwargs)
		Change the permissions of either the named file relative to this file
		descriptor or (if the path argument is omitted) the filesystem entry that
		this file descriptor itself refers to.

		All parameters are passed through to `os.chmod`.

		:param path: The path to change the permission bits of (optional).
		:type path: str or bytes
		:param int mode: The numeric permission bits to apply."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		if args < 2:
			return os_chmod(self, *args, dir_fd = None, **kwargs)
		else:
			path = args[0]
			if isinstance(path, int):
				raise RuntimeError("to chmod the fd itself, omit the fd parameter")
			return os_chmod(unpath(path), *args[1:], dir_fd = self, **kwargs)

	def chown(self, *args, **kwargs):
		"""chown([path,] uid, gid, *args, **kwargs)
		Change the ownership of either the named file relative to this file
		descriptor or (if the path argument is omitted) the filesystem entry that
		this file descriptor itself refers to.

		All parameters are passed through to `os.chown`.

		:param path: The path to change the ownership of (optional).
		:type path: str or bytes
		:param int uid: The numeric user id to apply.
		:param int gid: The numeric group id to apply."""

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
		"""Tell the operating system to flush file contents and metadata to
		stable storage."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		return os_fsync(self)

	def datasync(self):
		"""Tell the operating system to flush file contents to stable storage."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		return os_fdatasync(self)

	def pathconf(self, name):
		"""Return system configuration information relevant to the file referred to
		by this file descriptor. See `os.pathconf` for details.

		:param name: The name of the system configuration value to retrieve.
		:type name: str or int
		:return: The system configuration value."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		return os_pathconf(self, name)

	def statvfs(self):
		"""Gather information on the filesystem on which the directory entry
		referred to by this file descriptor resides. See `os.statvfs` for
		details.

		:return: Filesystem information."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		return os_statvfs(self)

	def truncate(self, arg, *args, **kwargs):
		"""truncate([path,] length, *args, **kwargs)
		Truncate either the named file relative to this file descriptor or (if the
		path argument is omitted) the filesystem entry that this file descriptor
		itself refers to, to the given size.

		:param path: The path of the file to truncate, relative to this file
			descriptor (optional).
		:type path: str or bytes or Path
		:param length: The size to truncate this file to.

		All parameters are passed to `os.truncate`."""

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
		"""Assess whether the current process would be able to access the given file.
		See `os.access` for details.

		:param path: The path to check, relative to this file descriptor.
		:type path: str or bytes or Path
		:return: Whether this path can be accessed.
		:rtype: bool

		All parameters are passed to `os.access`."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		return os_access(unpath(path), *args, dir_fd = self, **kwargs)

	def stat(self, *args, **kwargs):
		"""Retrieve filesystem metadata from the operating system for the named
		file relative to this file descriptor or (if the path argument is omitted)
		the filesystem entry that this file descriptor itself refers to.

		:return: Filesystem information for the directory entry.
		:rtype: os.stat_result

		All parameters are passed to `os.stat`."""

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
		"""readlink(path)
		Read the link target for a symlink, referred to by a path relative
		to this file descriptor.

		:param path: The path of the symlink, relative to this file descriptor.
		:type path: str or bytes or Path
		:return: The symlink's target.
		:rtype: str"""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		return os_readlink(unpath(path), *args, dir_fd = self, **kwargs)

	def utime(self, path, *args, **kwargs):
		utime([path,], *args, **kwargs)
		"""Change the access and modified times of either the named file relative
		to this file descriptor or (if the path argument is omitted) the filesystem
		entry that this file descriptor itself refers to.

		:param path: The path of the directory entry to modify, relative to this
			file descriptor (optional).
		:type path: str or bytes or Path

		All parameters are passed to `os.utime`. See `os.utime` for details."""

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
	def opener(self, mode = 0o666, **kwargs):
		"""Create an opener suitable for use with the builtin Python `open`
		function's `opener` parameter. See the documentation for Python's `open`
		for details about what this parameter is used for.

		This method returns an opener that will call `os.open` with the supplied
		`mode` and other arguments, with the `dir_fd` parameter set to itself, and
		with the `flags` arguments that Python's `open` will provide (bitwise or'd
		with ``O_CLOEXEC|O_NOCTTY``).

		:param int mode: Integer modes to pass to `os.open`.
		:param dict kwargs: Additional arguments that will be passed to `os.open`.
		:return: A function that is a suitable argument to Python's `open`."""

		def opener(path, flags):
			if self.closed:
				raise ValueError("I/O operation on closed file.")

			return os_open(path, flags|O_CLOEXEC|O_NOCTTY, mode = mode, dir_fd = self, **kwargs)
		return opener

	def open(self, path, flags, *args, **kwargs):
		"""Call `os.open` on a path relative to this file descriptor.

		:param path: The path to open, relative to this file descriptor.
		:type path: str or bytes or Path
		:return: An (unmanaged) file descriptor.
		:rtype: int"""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		return os_open(unpath(path), flags, *args, dir_fd = self, **kwargs)

	def mkdir(self, path, *args, exist_ok = False, **kwargs):
		"""Create a new directory with a path relative to this file descriptor.

		:param path: The path of the directory to create, relative to this file
			descriptor.
		:type path: str or bytes or Path
		:param bool exist_ok: Do not raise an exception if the directory already
			exists.

		All parameters are passed to `os.mkdir`. See `os.mkdir` for details."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		try:
			return os_mkdir(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def mkfifo(self, path, *args, exist_ok = False, **kwargs):
		"""Create a new named pipe with a path relative to this file descriptor.

		:param path: The path of the named pipe to create, relative to this file
			descriptor.
		:type path: str or bytes or Path
		:param bool exist_ok: Do not raise an exception if the named pipe already
			exists.

		All parameters are passed to `os.mkfifo`. See `os.mkfifo` for details."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		try:
			return os_mkfifo(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def mknod(self, path, *args, exist_ok = False, **kwargs):
		"""Create a new directory entry with a path relative to this file descriptor.

		:param path: The path of the directory entry to create, relative to this file
			descriptor.
		:type path: str or bytes or Path
		:param bool exist_ok: Do not raise an exception if the directory entry already
			exists.

		All parameters are passed to `os.mknod`. See `os.mknod` for details."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		try:
			return os_mknod(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def symlink(self, src, *args, exist_ok = False, **kwargs):
		"""Create a new symbolic link with a path relative to this file descriptor.

		:param path: The path of the symbolic link to create, relative to this file
			descriptor.
		:type path: str or bytes or Path
		:param bool exist_ok: Do not raise an exception if the symbolic link already
			exists.

		All parameters are passed to `os.symlink`. See `os.symlink` for details."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		try:
			return os_symlink(unpath(path), *args, dir_fd = self, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def unlink(self, path, *, missing_ok = False, **kwargs):
		"""Remove a (non-directory) filesystem entry, relative to this file
		descriptor.

		:param path: The path to remove, relative to this file descriptor.
		:type path: str or bytes or Path
		:param bool missing_ok: Do not raise an exception if the directory entry
			does not exist."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		try:
			return os_unlink(unpath(path), dir_fd = self, **kwargs)
		except FileNotFoundError:
			if not missing_ok:
				raise

	def remove(self, path, *, missing_ok = False, **kwargs):
		"""Remove a (non-directory) filesystem entry, relative to this file
		descriptor.

		:param path: The path to remove, relative to this file descriptor.
		:type path: str or bytes or Path
		:param bool missing_ok: Do not raise an exception if the directory entry
			does not exist."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		try:
			return os_remove(unpath(path), dir_fd = self, **kwargs)
		except FileNotFoundError:
			if not missing_ok:
				raise

	def rmdir(self, path, **kwargs):
		"""Remove a directory, relative to this file descriptor.

		:param path: The path to remove, relative to this file descriptor.
		:type path: str or bytes or Path
		:param bool missing_ok: Do not raise an exception if the directory does not
			exist."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		try:
			return os_rmdir(unpath(path), dir_fd = self, **kwargs)
		except FileNotFoundError:
			if not missing_ok:
				raise

	def rename(self, src, dst, *, dir_fd = None, **kwargs):
		"""Rename a directory entry relative to this file descriptor. If the `dir_fd`
		named argument is used, the destination path `dst` will be relative to that
		file descriptor. Otherwise, both `src` and `dst` are relative to the file
		descriptor you invoked this method on.

		:param src: The path of the (existing) directory entry to move.
		:type src: str or bytes or Path
		:param dst: The new path of the directory entry.
		:type dst: str or bytes or Path
		:param int dir_fd: Interpret `dst` relative to this file descriptor."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		if dir_fd is None:
			dir_fd = self

		return os_rename(unpath(src), unpath(dst), src_dir_fd = self, dst_dir_fd = dir_fd, **kwargs)

	def replace(self, src, dst, *, dir_fd = None, **kwargs):
		"""Replace a directory entry relative to this file descriptor. If the `dir_fd`
		named argument is used, the destination path `dst` will be relative to that
		file descriptor. Otherwise, both `src` and `dst` are relative to the file
		descriptor you invoked this method on.

		:param src: The path of the (existing) directory entry to move.
		:type src: str or bytes or Path
		:param dst: The new path of the directory entry.
		:type dst: str or bytes or Path
		:param int dir_fd: Interpret `dst` relative to this file descriptor."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		if dir_fd is None:
			dir_fd = self
		return os_replace(unpath(src), unpath(dst), src_dir_fd = self, dst_dir_fd = dir_fd, **kwargs)

	def link(self, src, dst, *, dir_fd = None, exist_ok = False, **kwargs):
		"""Create a hardlink relative to this file descriptor. If the `dir_fd`
		named argument is used, the destination path `dst` will be relative to that
		file descriptor. Otherwise, both `src` and `dst` are relative to the file
		descriptor you invoked this method on.

		:param src: The path of the (existing) directory entry.
		:type src: str or bytes or Path
		:param dst: The path of the hardlink to create.
		:type dst: str or bytes or Path
		:param int dir_fd: Interpret `dst` relative to this file descriptor."""

		if self.closed:
			raise ValueError("I/O operation on closed file.")

		if dir_fd is None:
			dir_fd = self

		try:
			return os_link(unpath(src), unpath(dst), src_dir_fd = self, dst_dir_fd = dir_fd, **kwargs)
		except FileExistsError:
			if not exist_ok:
				raise

	def flock(self, operation = LOCK_EX):
		"""Create and return a context manager of an fcntl lock on this file
		descriptor. See `fruitbak.util.fd.flock` for details.

		:param operation int: The type of operation. Defaults to LOCK_EX.
		:return: the `flock` object that holds the lock.
		:rtype: fruitbak.util.fd.flock"""

		return flock(self, operation)

sysopen = fd.sysopen
"""Open a file using `os.open` and return it as an `fd` instance.

All additional parameters are passed to `os.open`.

:param path: The filesystem path to open.
:type path: str or bytes or Path
:param int flags: Flags for `os.open`
:param int mode: Permission bits for files created by `os.open`.
:param bool large_file: Add `O_LARGEFILE` to `flags`.
:param bool follow_symlinks: Do not add `O_NOFOLLOW` to `flags`.
:param bool controlling_tty: Do not add `O_NOCTTY` to `flags`.
:param bool inheritable: Do not add `O_CLOEXEC` to `flags`.
:return: The wrapped fd for the opened file.
:rtype: fruitbak.util.fd.fd"""

sysopendir = fd.sysopendir
"""Open a directory using `os.open` and return it as an `fd` instance.

All additional parameters are passed to `sysopen`.

:param path: The filesystem path to open.
:type path: str or bytes or Path
:param int mode: Permission bits for directories created by this method.
:param bool path_only: Add `O_PATH` to `flags`.
:param bool create_ok: Create the directory if it does not exist.
:return: The fd for the opened directory.
:rtype: fruitbak.util.fd.fd"""

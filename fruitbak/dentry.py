"""Represent entries in a filesystem."""

from fruitbak.util.clarity import Clarity, initializer
from stat import *

class DentryError(Exception):
	"""Something Dentry-related went wrong."""
	pass

class FileTypeError(DentryError, ValueError):
	pass

class NotAHardlinkError(FileTypeError):
	pass

class NotASymlinkError(FileTypeError):
	pass

class NotADeviceError(FileTypeError):
	pass

class NotABlockDeviceError(NotADeviceError):
	pass

class NotACharacterDeviceError(NotADeviceError):
	pass

def _to_bytes(value):
	"""Convert various values to the format suitable for storing in Hardhats."""
	if isinstance(value, bytes):
		return value
	elif isinstance(value, str):
		return value.encode()
	else:
		return bytes(value)

class Dentry(Clarity):
	"""Represent entries in a filesystem.

	Objects of this type represent entries in a filesystem, including the name,
	metadata such as file size and last modification time, as well as file type
	specific information such as symlink destinations or block device major
	and minor numbers.

	These objects are used in Fruitbak to represent filesystem entries both
	when they are stored as part of the process of creating a backup, and when
	listing or retrieving files in an existing backup.

	Specific to Fruitbak is the digests information: the list of digests of the
	data chunks that when concatenated form the contents of the file.

	Hardlinks in Fruitbak are handled in a way that is more similar to symlinks
	than the usual unix system of inode indirection. Hardlinks do not have
	target file type specific information (such as digest lists) themselves; to
	get that information you need to retrieve the entry using the name returned
	by the hardlink function.

	All metadata (such as size, file ownership, file type, etcetera) is stored
	with the hardlink as it was found on the filesystem, which means it is
	usually the same as the hardlink destination. It may differ if, for
	example, the file was modified between backing up the hardlink and its
	target.

	Please note that most unix implementations allow you to create hardlinks
	to not only plain files but also to block/character device nodes, named
	pipes, unix domain sockets and even to symlinks. Only hardlinks to
	directories are generally not possible. Fruitbak follows this model.

	Some functions that return a Dentry may return a Hardlink object instead,
	which is a convenience wrapper that behaves more like a hardlink would on a
	unix filesystem: functions to access the filetype specific data will return
	data from the hardlink target instead.
	"""

	MAXNAMELEN = 65535
	FORMAT_FLAG_HARDLINK = 0x1 
	FORMAT_MASK = FORMAT_FLAG_HARDLINK

	@initializer
	def extra(self):
		return bytearray()

	@initializer
	def is_hardlink(self):
		"""Is this dentry a hardlink?

		If so, you can use the hardlink property to see what path it points at.

		Boolean, readwrite, defaults to False.
		"""

		return False

	@is_hardlink.setter
	def is_hardlink(self, value):
		return True if value else False

	@property
	def hardlink(self):
		"""The path this hardlink points at.

		Bytes, readwrite.

		Raises a NotAHardlinkError exception if this dentry is not a hardlink.
		"""

		if self.is_hardlink:
			return self.extra
		raise NotAHardlinkError("'%s' is not a hardlink" % self.name)

	@hardlink.setter
	def hardlink(self, value):
		if self.is_hardlink:
			self.extra = _to_bytes(value)
		elif hasattr(self, 'extra'):
			raise NotAHardlinkError("'%s' is not a hardlink" % self.name)
		elif self.is_directory:
			raise NotAHardlinkError("'%s' is not a hardlink" % self.name)
		else:
			self.extra = _to_bytes(value)
			self.is_hardlink = True

	@property
	def is_symlink(self):
		"""Is this dentry a symbolic link?

		Boolean, readonly.

		If so, you can use the symlink property to see what path it points at.
		"""

		return S_ISLNK(self.mode)

	@property
	def symlink(self):
		"""The path this symlink points at.

		Bytes, readwrite.

		Raises a NotASymlinkError exception if this dentry is not a symlink.
		"""

		if self.is_symlink:
			return self.extra
		raise NotASymlinkError("'%s' is not a hardlink" % self.name)

	@property
	def is_directory(self):
		"""Is this dentry a directory?

		Boolean, readonly.
		"""

		return S_ISDIR(self.mode)

	@property
	def is_device(self):
		"""Is this dentry a device?

		Boolean, readonly.
		"""

		mode = self.mode
		return S_ISCHR(mode) or S_ISBLK(mode)

	@property
	def is_chardev(self):
		"""Is this dentry a character device?

		Boolean, readonly.
		"""

		return S_ISCHR(self.mode)

	@property
	def is_blockdev(self):
		"""Is this dentry a block device?

		Boolean, readonly.
		"""

		return S_ISBLK(self.mode)

	@property
	def rdev_major(self):
		if self.is_device:
			major, minor = unpack('<LL', self.extra)
			return major
		raise NotADeviceError("'%s' is not a device" % self.name)

	@rdev_major.setter
	def rdev_major(self, major):
		if self.is_device:
			extra = self.extra
			if extra:
				old_major, minor = unpack('<LL', self.extra)
			else:
				minor = 0
			self.extra[:] = pack('<LL', major, minor)
		raise NotADeviceError("'%s' is not a device" % self.name)

	@property
	def rdev_minor(self):
		if self.is_device:
			major, minor = unpack('<LL', self.extra)
			return minor
		raise NotADeviceError("'%s' is not a device" % self.name)

	@rdev_minor.setter
	def rdev_minor(self, minor):
		if self.is_device:
			extra = self.extra
			if extra:
				major, old_minor = unpack('<LL', self.extra)
			else:
				major = 0
			self.extra[:] = pack('<LL', major, minor)
		raise NotADeviceError("'%s' is not a device" % self.name)

	@property
	def rdev(self):
		if self.is_device:
			return unpack('<LL', self.extra)
		raise NotADeviceError("'%s' is not a device" % self.name)

	@rdev.setter
	def rdev(self, majorminor):
		if self.is_device:
			self.extra[:] = pack('<LL', *majorminor)
		raise NotADeviceError("'%s' is not a device" % self.name)

	@property
	def is_fifo(self):
		"""Is this dentry a named pipe?

		Boolean, readonly.
		"""

		return S_ISFIFO(self.mode)

	@property
	def is_socket(self):
		"""Is this dentry a unix domain socket?

		Boolean, readonly.
		"""

		return S_ISSOCK(self.mode)

class HardlinkDentry(Dentry):
	def __init__(original, target):
		super().__init__(original = original, target = target)

	@property
	def name(self):
		return self.original.name

	@property
	def inode(self):
		return self.target.inode

	@property
	def mode(self):
		return self.target.mode

	@property
	def size(self):
		return self.target.size

	@property
	def storedsize(self):
		return self.target.storedsize

	@property
	def mtime_ns(self):
		return self.target.mtime_ns

	@property
	def uid(self):
		return self.target.uid

	@property
	def gid(self):
		return self.target.gid

	@property
	def digests(self):
		return self.target.digests

	@property
	def hardlink(self):
		return self.target.name

	@property
	def symlink(self):
		return self.target.symlink

	@property
	def rdev_minor(self):
		return self.target.rdev_minor

	@property
	def rdev_major(self):
		return self.target.rdev_major

	@property
	def extra(self):
		return self.target.extra

	@property
	def is_hardlink(self):
		return True

	@property
	def is_file(self):
		return self.target.is_file

	@property
	def is_directory(self):
		return False

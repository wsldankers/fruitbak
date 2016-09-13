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

class NotABlockDeviceError(FileTypeError):
	pass

class NotACharacterDeviceError(FileTypeError):
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

	@initializer
	def is_hardlink(self):
		"""Is this dentry a hardlink?

		If so, you can use the hardlink property to see what path it points at.

		Boolean, readonly, defaults to False.
		"""

		return False

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

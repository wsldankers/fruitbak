"""Represent entries in a filesystem.

Dentry objects represent entries in a filesystem, including the name,
metadata such as file size and last modification time, as well as file type
specific information such as symlink destinations or block device major
and minor numbers.

These objects are used in Fruitbak to represent filesystem entries both
when they are stored as part of the process of creating a backup, and when
listing or retrieving files in an existing backup.

Specific to Fruitbak is the hashes information: the list of hashes of the
data chunks that when concatenated form the contents of the file.

Hardlinks in Fruitbak are handled in a way that is more similar to symlinks
than the usual unix system of inode indirection. Hardlinks do not have
target file type specific information (such as hash lists) themselves; to
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

from stat import *
from io import RawIOBase, TextIOWrapper
from struct import pack, unpack, Struct
from typing import Optional

from fruitbak.util import Initializer, initializer, ensure_byteslike, ensure_str

INT64_MIN = -2**63
INT64_MAX = 2**63 - 1

class DentryError(Exception):
	"""Something Dentry-related went wrong.

	This is merely the base class for all Dentry exceptions.
	It is never thrown as-is."""

class FileTypeError(DentryError, ValueError):
	"""An operation was attempted on a Dentry object that is meant for a
	different type of entry (as determined by the `mode` attribute.
	For example, an attempt to set the file contents for a symlink.

	This is merely the base class for other Dentry exceptions.
	It is never thrown as-is.

	Subclass of :class:`DentryError` and `ValueError`."""

class NotAFileError(FileTypeError):
	"""An operation was attempted on a Dentry object representing a regular
	file that is not applicable to regular files.

	Subclass of :class:`FileTypeError`."""

class NotAHardlinkError(FileTypeError):
	"""An operation was attempted on a Dentry object representing a hardlink
	that is not applicable to hardlinks.

	Subclass of :class:`FileTypeError`."""

class NotASymlinkError(FileTypeError):
	"""An operation was attempted on a Dentry object representing a symlink
	that is not applicable to symlinks.

	Subclass of :class:`FileTypeError`."""

class NotADeviceError(FileTypeError):
	"""An operation was attempted on a Dentry object representing a device
	that is not applicable to devices.

	This is merely the base class for the more specific device related
	type errors below. It is never thrown as-is.

	Subclass of :class:`FileTypeError`."""

class NotABlockDeviceError(NotADeviceError):
	"""An operation was attempted on a Dentry object representing a block
	device that is not applicable to (block) devices.

	Subclass of :class:`NotADeviceError`."""

class NotACharacterDeviceError(NotADeviceError):
	"""An operation was attempted on a Dentry object representing a character
	device that is not applicable to (character) devices.

	Subclass of :class:`NotADeviceError`."""

class DentryUnsupportedFlag(DentryError):
	"""An unsupported flag was encountered while decoding the dentry wire
	format. This entry was either created with a newer version of Fruitbak
	or is corrupted.

	Subclass of :class:`DentryError`."""

dentry_layout = Struct('<LLQqLL')
"""The header of the wire format. For internal use."""
dentry_layout_size = dentry_layout.size
"""Size of the header of the wire format. For internal use."""

DENTRY_FORMAT_FLAG_HARDLINK = 0x1
"""

Wire format bitmask flag denoting that this dentry is a hardlink.

"""
DENTRY_FORMAT_SUPPORTED_FLAGS = DENTRY_FORMAT_FLAG_HARDLINK
"""Bitmask of all supported format flags. Any dentries with flags not in
this bitmask will be rejected."""

class DENTRY_TYPE(Initializer):
	"""Represent the file types supported by Fruitbak. Returned by the
	Dentry type() method. Note that hardlinks are a seperate type in
	Fruitbak, so an entry that is a hardlink to a regular file is
	classified as a hardlink and not a regular file.

	Base class for the DENTRY_TYPE_* classes. Never used directly.

	Each DENTRY_TYPE_* class defines several representations of the file
	type for use in various contexts:"""

	lsl_char: Optional[str] = None
	"""The character used in the output of of ``ls -l``. Read-only.

	:type: str or None"""

	tar_char: Optional[bytes] = None
	"""The byte used in tar archives. Read-only.

	:type: bytes or None"""

	stat_num: Optional[int] = None
	"""The number used in the S_IFMT part of the stat() st_mode field.
	Read-only.

	:type: int or None"""

class DENTRY_TYPE_UNKNOWN(DENTRY_TYPE):
	"""Used for dentries of unknown type."""
	lsl_char = '?'

class DENTRY_TYPE_FILE(DENTRY_TYPE):
	"""Used for dentries that are regular files."""
	lsl_char = '-'
	tar_char = b'0'
	stat_num = S_IFREG

class DENTRY_TYPE_HARDLINK(DENTRY_TYPE):
	"""Used for dentries that are hardlinks. The target may be any file
	type except a directory."""
	lsl_char = 'h'
	tar_char = b'1'

class DENTRY_TYPE_SYMLINK(DENTRY_TYPE):
	"""Used for dentries that are symlinks."""
	lsl_char = 'l'
	tar_char = b'2'
	stat_num = S_IFLNK

class DENTRY_TYPE_DEVICE(DENTRY_TYPE):
	"""Used for dentries that are either block devices or character devices.

	Base class for the DENTRY_TYPE_*DEVICE classes. Never used directly."""

class DENTRY_TYPE_CHARDEVICE(DENTRY_TYPE_DEVICE):
	"""Used for dentries that are character devices."""
	lsl_char = 'c'
	tar_char = b'3'
	stat_num = S_IFCHR

class DENTRY_TYPE_BLOCKDEVICE(DENTRY_TYPE_DEVICE):
	"""Used for dentries that are block devices."""
	lsl_char = 'b'
	tar_char = b'4'
	stat_num = S_IFBLK

class DENTRY_TYPE_DIRECTORY(DENTRY_TYPE):
	"""Used for dentries that are directories."""
	lsl_char = 'd'
	tar_char = b'5'
	stat_num = S_IFDIR

class DENTRY_TYPE_FIFO(DENTRY_TYPE):
	"""Used for dentries that are named pipes."""
	lsl_char = 'p'
	tar_char = b'6'
	stat_num = S_IFIFO

class DENTRY_TYPE_SOCKET(DENTRY_TYPE):
	"""Used for dentries that are UNIX domain sockets."""
	lsl_char = 'p'
	stat_num = S_IFSOCK

dentry_types_by_stat_num = {t.stat_num: t for t in (
	DENTRY_TYPE_FILE,
	DENTRY_TYPE_SYMLINK,
	DENTRY_TYPE_CHARDEVICE,
	DENTRY_TYPE_BLOCKDEVICE,
	DENTRY_TYPE_DIRECTORY,
	DENTRY_TYPE_FIFO,
	DENTRY_TYPE_SOCKET
)}
"""Private variable that maps stat() st_mode numbers to the correct
DENTRY_TYPE_* class."""

class DentryIO(RawIOBase):
	"""Private class that provides a read-only wrapper around Dentry
	objects so that they can be used as Python file handle objects.

	:param iter(bytes) readahead: any iterator that yields
			byteslike objects (usually a fruitbak.pool.agent.PoolReadahead)."""

	current_chunk = None
	"""The last read chunk, used to satisfy reads that are not exactly
	the Fruitbak chunk size or are not aligned to the chunk size.
	May be None if there is the last chunk was completely read (or no
	chunks have been read yet).

	:rtype: memoryview or None"""
	current_offset = 0
	"""Offset in the current chunk; always strictly smaller than the
	length of current_chunk.

	:type: int"""

	def __init__(self, readahead):
		self.readahead = readahead

	def readable(self):
		"""Whether this IO object is readable (always True).

		:rtype: bool"""
		return True

	def readall(self):
		"""Read all that is left of the readahead iterator and return
		it as one big bytes object.

		:rtype: bytes"""
		chunks = []
		current_chunk = self.current_chunk
		if current_chunk is not None:
			chunks.append(current_chunk[self.current_offset:])
			del self.current_chunk
			del self.current_offset
		chunks.extend(self.readahead)
		return b''.join(chunks)

	def read(self, size = -1):
		"""Read `size` bytes from the readahead iterator.
		If `size` is -1 (or None), read all that is left of the
		readahead iterator and return it as one big bytes object.

		:param int size: the number of bytes to read
		:rtype: bytes"""
		if size is None or size < 0:
			return self.readall()
		if size == 0:
			return b''

		current_chunk = self.current_chunk
		if current_chunk is None:
			try:
				current_chunk = next(self.readahead)
			except StopIteration:
				return b''
			else:
				current_chunk = memoryview(current_chunk.value)
			self.current_chunk = current_chunk
			current_offset = 0
			next_offset = size
		else:
			current_offset = self.current_offset
			next_offset = current_offset + size

		if next_offset >= len(current_chunk):
			try:
				del self.current_chunk
			except AttributeError:
				pass
			try:
				del self.current_offset
			except AttributeError:
				pass
			return current_chunk[self.current_offset:]
		else:
			self.current_offset = current_offset + size
			return current_chunk[current_offset:next_offset]

	def readinto(self, b):
		"""Read from the readahead iterator and store the data in the supplied
		memoryview `b`, until either the memoryview is full or the readahead
		iterator is exhausted."""
		if not isinstance(b, memoryview):
			b = memoryview(b)
		b = b.cast('B')

		# This is inefficient, it would be better to copy from the iterated
		# chunks directly.
		data = self.read(len(b))
		n = len(data)

		b[:n] = data

		return n

class DentryHashes(Initializer):
	"""DentryHashes(*, hashes = None, hash_size = None)
	Wrapper for concatenated hashes that allows iteration as well as
	retrieving the underlying byteslike object.

	Iterating over this object yields all hashes in turn.

	Using len() will return the number of hashes.

	Casting it to a bytes() object will return the concatenation of all hashes.

	:param byteslike hashes: the concatenation of hashes (required)
	:param int hash_size: the size of each hash (required)
	"""

	@initializer
	def _hashview(self):
		"""A memoryview of the underlying byteslike object."""
		m = self.hashes
		if isinstance(m, memoryview):
			return m
		return memoryview(m)

	def __iter__(self):
		hash_size = self.hash_size
		def hashiterator(hashes):
			offset = 0
			length = len(hashes)
			while offset < length:
				next_offset = offset + hash_size
				yield hashes[offset:next_offset]
				offset = next_offset
		return hashiterator(self._hashview)

	def __len__(self):
		return len(self.hashes) // self.hash_size

	def __getitem__(self, index):
		num = len(self)
		if index < 0:
			new_index = index + num
			if new_index < 0:
				raise IndexError(f"Index {index} out of range")
			index = new_index
		elif index >= num:
			raise IndexError(f"Index {index} out of range")
		hash_size = self.hash_size
		offset = index * hash_size
		return self._hashview[offset : offset + hash_size]

	def __bytes__(self):
		return self.hashes

class Dentry(Initializer):
	"""Represent entries in a filesystem. Can be initialized by either
	passing a wire format encoded byteslike object as `encoded` or
	by setting all desired attributes manually.

	`Dentry` objects can be encoded to the wire format by casting
	it to a bytes() object.

	:param byteslike encoded: dentry data in wire format
	"""

	def __init__(self, encoded = None, **kwargs):
		if encoded is not None:
			flags, self.mode, self.size, self.mtime, self.uid, self.gid = dentry_layout.unpack_from(encoded)
			unsupported_flags = flags & ~DENTRY_FORMAT_SUPPORTED_FLAGS
			if unsupported_flags:
				raise DentryUnsupportedFlag('unsupported flag in encoded entry: %x' % unsupported_flags)
			if flags & DENTRY_FORMAT_FLAG_HARDLINK:
				self.is_hardlink = True
			self.extra = encoded[dentry_layout_size:]

		super().__init__(**kwargs)

	def __bytes__(self):
		flags = 0
		if self.is_hardlink:
			flags |= DENTRY_FORMAT_FLAG_HARDLINK
		return dentry_layout.pack(
			flags,
			self.mode,
			self.size,
			min(INT64_MAX, max(INT64_MIN, self.mtime)),
			self.uid,
			self.gid,
		) + self.extra

	@initializer
	def name(self):
		"""The name of this dentry.

		:type: str or byteslike"""
		raise AttributeError("attempt to access 'name' attribute before it is set")

	@name.setter
	def name(self, value):
		return ensure_byteslike(value)

	@initializer
	def extra(self):
		"""The extra data for this entry, in wire format. Contents depend
		on the file type.

		:type: byteslike"""
		return bytearray()

	@initializer
	def fruitbak(self):
		return self.share.fruitbak

	@initializer
	def pool(self):
		return self.fruitbak.pool

	@initializer
	def agent(self):
		return self.pool.agent()

	@initializer
	def hash_size(self):
		return self.fruitbak.hash_size

	@initializer
	def chunk_size(self):
		return self.fruitbak.chunk_size

	def open(self, mode = 'rb', agent = None):
		"""Open the dentry for reading. Will raise an error if this
		dentry is not a regular file.

		:param str mode: open mode, must be either 'r' or 'rb'
		:param PoolAgent agent: a pool agent to use for reading
		:rtype: IOBase
		"""
		if agent is None:
			agent = self.agent
		io = DentryIO(readahead = agent.readahead(self.hashes))
		if mode == 'rb':
			return io
		elif mode == 'r':
			wrapper = TextIOWrapper(io)
			wrapper._CHUNK_SIZE = self.chunk_size
			return wrapper
		else:
			return RuntimeError("Unsupported open mode %r" % (mode,))

	@property
	def type(self):
		return dentry_types_by_stat_num.get(S_IFMT(self.mode), DENTRY_TYPE_UNKNOWN)

	@property
	def is_file(self):
		return S_ISREG(self.mode)

	@property
	def is_file(self):
		return S_ISREG(self.mode)

	@property
	def hashes(self):
		"""The hashes for this file.

		DentryHashes, readwrite.

		Raises a NotAFileError exception if this dentry is not a regular file.
		"""

		if not self.is_file:
			raise NotAFileError("'%s' is not a regular file" % ensure_str(self.name))
		return DentryHashes(hashes = self.extra, hash_size = self.hash_size)

	@hashes.setter
	def hashes(self, value):
		if not self.is_file:
			raise NotAFileError("'%s' is not a regular file" % ensure_str(self.name))

		if isinstance(value, DentryHashes):
			self.extra = value.hashes
		else:
			try:
				memoryview(value)
			except:
				self.extra = b''.join(value)
			else:
				self.extra = value

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

		if not self.is_hardlink:
			raise NotAHardlinkError("'%s' is not a hardlink" % ensure_str(self.name))

		return self.extra

	@hardlink.setter
	def hardlink(self, value):
		if not self.is_hardlink:
			raise NotAHardlinkError("'%s' is not a hardlink" % ensure_str(self.name))

		self.extra = ensure_byteslike(value)

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

		if not self.is_symlink:
			raise NotASymlinkError("'%s' is not a hardlink" % ensure_str(self.name))
		return self.extra

	@symlink.setter
	def symlink(self, value):
		if not self.is_symlink:
			raise NotASymlinkError("'%s' is not a hardlink" % ensure_str(self.name))

		self.extra = ensure_byteslike(value)

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
		return self.rdev[0]

	@rdev_major.setter
	def rdev_major(self, major):
		minor = self.rdev[1] if self.extra else 0
		self.extra = pack('<LL', major, minor)

	@property
	def rdev_minor(self):
		return self.rdev[1]

	@rdev_minor.setter
	def rdev_minor(self, minor):
		major = self.rdev[0] if self.extra else 0
		self.extra = pack('<LL', major, minor)

	@property
	def rdev(self):
		if not self.is_device:
			if self.is_chardev:
				raise NotACharacterDeviceError("'%s' is not a (character) device" % ensure_str(self.name))
			else:
				raise NotABlockDeviceError("'%s' is not a (block) device" % ensure_str(self.name))
		major, minor = unpack('<LL', self.extra)
		if not major and minor & ~0xFF:
			# compensate for old bug:
			return minor >> 8, minor & 0xFF
		else:
			return major, minor

	@rdev.setter
	def rdev(self, majorminor):
		if not self.is_device:
			if self.is_chardev:
				raise NotACharacterDeviceError("'%s' is not a (character) device" % ensure_str(self.name))
			else:
				raise NotABlockDeviceError("'%s' is not a (block) device" % ensure_str(self.name))
		self.extra = pack('<LL', *majorminor)

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

# not a subclass of Dentry because then __getattr__ would not be called.
class HardlinkDentry(Initializer):
	def __init__(self, original, target, **kwargs):
		super().__init__(original = original, target = target, **kwargs)

	@property
	def name(self):
		return self.original.name

	@property
	def hardlink(self):
		return self.original.hardlink

	def __getattr__(self, name):
		return getattr(self.target, name)

	is_hardlink = True
	is_directory = False

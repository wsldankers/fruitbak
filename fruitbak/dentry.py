"""Represent entries in a filesystem."""

from fruitbak.util import Clarity, initializer
from stat import *
from io import RawIOBase, TextIOWrapper
from struct import pack, unpack, Struct

def dentry_encode(filename):
	"""
	Encode filename to UTF-8 encoding with 'surrogateescape' error handler,
	return bytes-like ojects unchanged.
	"""
	if isinstance(filename, str):
		return filename.encode(errors = 'surrogateescape')
	try:
		memoryview(filename)
	except:
		raise TypeError("expect bytes or str, not %s" % type(filename).__name__) from None
	else:
		return filename

def dentry_decode(filename):
	"""
	Decode filename from UTF-8 encoding with 'surrogateescape' error handler,
	return str unchanged.
	"""
	if isinstance(filename, str):
		return filename
	try:
		decode = filename.decode
	except AttributeError:
		raise TypeError("expect bytes or str, not %s" % type(filename).__name__)
	else:
		return filename.decode(errors = 'surrogateescape')

class DentryError(Exception):
	"""Something Dentry-related went wrong."""
	pass

class FileTypeError(DentryError, ValueError):
	pass

class NotAFileError(FileTypeError):
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

class DentryUnsupportedFlag(DentryError):
	pass

dentry_layout = Struct('<LLQQLL')
dentry_layout_size = dentry_layout.size

DENTRY_FORMAT_FLAG_HARDLINK = 0x1
DENTRY_FORMAT_SUPPORTED_FLAGS = DENTRY_FORMAT_FLAG_HARDLINK

class DENTRY_TYPE(Clarity):
	lsl_char = None
	tar_char = None
	stat_num = None

class DENTRY_TYPE_UNKNOWN(DENTRY_TYPE):
	lsl_char = '?'

class DENTRY_TYPE_FILE(DENTRY_TYPE):
	lsl_char = '-'
	tar_char = b'0'
	stat_num = S_IFREG

class DENTRY_TYPE_HARDLINK(DENTRY_TYPE):
	lsl_char = 'h'
	tar_char = b'1'

class DENTRY_TYPE_SYMLINK(DENTRY_TYPE):
	lsl_char = 'l'
	tar_char = b'2'
	stat_num = S_IFLNK

class DENTRY_TYPE_DEVICE(DENTRY_TYPE):
	pass

class DENTRY_TYPE_CHARDEVICE(DENTRY_TYPE_DEVICE):
	lsl_char = 'c'
	tar_char = b'3'
	stat_num = S_IFCHR

class DENTRY_TYPE_BLOCKDEVICE(DENTRY_TYPE_DEVICE):
	lsl_char = 'b'
	tar_char = b'4'
	stat_num = S_IFBLK

class DENTRY_TYPE_DIRECTORY(DENTRY_TYPE):
	lsl_char = 'd'
	tar_char = b'5'
	stat_num = S_IFDIR

class DENTRY_TYPE_FIFO(DENTRY_TYPE):
	lsl_char = 'p'
	tar_char = b'6'
	stat_num = S_IFIFO

class DENTRY_TYPE_SOCKET(DENTRY_TYPE):
	lsl_char = 'p'
	stat_num = S_IFSOCK

dentry_types_by_stat_num = dict(map(lambda t: (t.stat_num, t), (
	DENTRY_TYPE_FILE,
	DENTRY_TYPE_SYMLINK,
	DENTRY_TYPE_CHARDEVICE,
	DENTRY_TYPE_BLOCKDEVICE,
	DENTRY_TYPE_DIRECTORY,
	DENTRY_TYPE_FIFO,
	DENTRY_TYPE_SOCKET
)))

class DentryIO(RawIOBase):
	current_chunk = None # always a memoryview
	current_offset = 0

	def __init__(self, readahead):
		self.readahead = readahead

	def readable(self):
		return True

	def readall(self):
		chunks = []
		current_chunk = self.current_chunk
		if current_chunk is not None:
			chunks.append(current_chunk[self.current_offset:])
			del self.current_chunk
			del self.current_offset
		chunks.extend(self.readahead)
		return b''.join(chunks)

	def read(self, size = -1):
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
		if not isinstance(b, memoryview):
			b = memoryview(b)
		b = b.cast('B')

		data = self.read(len(b))
		n = len(data)

		b[:n] = data

		return n

class DentryDigests(Clarity):
	@initializer
	def digestview(self):
		m = self.digests
		if isinstance(m, memoryview):
			return m
		return memoryview(m)

	def __iter__(self):
		hashsize = self.hashsize
		def digestiterator(digests):
			offset = 0
			length = len(digests)
			while offset < length:
				next_offset = offset + hashsize
				yield digests[offset:next_offset]
				offset = next_offset
		return digestiterator(self.digestview)

	def __len__(self):
		return len(self.digests) // self.hashsize

	def __bytes__(self):
		return self.digests

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
			self.mtime,
			self.uid,
			self.gid,
		) + self.extra

	@initializer
	def name(self):
		raise AttributeError("attempt to access 'name' attribute before it is set")

	@name.setter
	def name(self, value):
		return dentry_encode(value)

	@initializer
	def extra(self):
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
	def hashsize(self):
		return self.fruitbak.hashsize

	def open(self, mode = 'rb', agent = None):
		if agent is None:
			agent = self.agent
		io = DentryIO(readahead = agent.readahead(self.digests))
		if mode == 'rb':
			return io
		elif mode == 'r':
			wrapper = TextIOWrapper(io)
			wrapper._CHUNK_SIZE = self.fruitbak.chunksize
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
	def digests(self):
		"""The digests for this file.

		DentryDigests, readwrite.

		Raises a NotAFileError exception if this dentry is not a regular file.
		"""

		if not self.is_file:
			raise NotAFileError("'%s' is not a regular file" % dentry_decode(self.name))
		return DentryDigests(digests = self.extra, hashsize = self.hashsize)

	@digests.setter
	def digests(self, value):
		if not self.is_file:
			raise NotAFileError("'%s' is not a regular file" % dentry_decode(self.name))

		if isinstance(value, DentryDigests):
			self.extra = value.digests
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
			raise NotAHardlinkError("'%s' is not a hardlink" % dentry_decode(self.name))

		return self.extra

	@hardlink.setter
	def hardlink(self, value):
		if not self.is_hardlink:
			raise NotAHardlinkError("'%s' is not a hardlink" % dentry_decode(self.name))

		self.extra = dentry_encode(value)

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
			raise NotASymlinkError("'%s' is not a hardlink" % dentry_decode(self.name))
		return self.extra

	@symlink.setter
	def symlink(self, value):
		if not self.is_symlink:
			raise NotASymlinkError("'%s' is not a hardlink" % dentry_decode(self.name))

		self.extra = dentry_encode(value)

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
			raise NotADeviceError("'%s' is not a device" % dentry_decode(self.name))
		major, minor = unpack('<LL', self.extra)
		if not major and minor & ~0xFF:
			# compensate for old bug:
			return minor >> 8, minor & 0xFF
		else:
			return major, minor

	@rdev.setter
	def rdev(self, majorminor):
		if not self.is_device:
			raise NotADeviceError("'%s' is not a device" % dentry_decode(self.name))
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

class HardlinkDentry(Dentry):
	def __init__(self, original, target, **kwargs):
		super().__init__(original = original, target = target, **kwargs)

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
	def mtime(self):
		return self.target.mtime

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

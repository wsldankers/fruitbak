from fruitbak.util import Initializer, initializer, locked, fd
from traceback import print_exc
from stat import S_IFREG, S_IFDIR
from fusepy import FuseOSError, Operations as FuseOperations
from errno import ENOENT
from threading import Lock
from os import dup
from sys import stderr
from functools import wraps

class FruitFuseFile(Initializer):
	dentry = None
	chunk = None
	chunk_index = None

def windshield(f):
	"""Catches bugs."""
	name = f.__name__
	@wraps(f)
	def windshield(self, *args, **kwargs):
		try:
			#self._trace(name, *args)
			return f(self, *args, **kwargs)
		except FuseOSError:
			raise
		except:
			print_exc(file = self._stderr)
		raise FuseOSError(ENOENT)
	return windshield

class FruitFuse(FuseOperations):
	use_ns = True

	def __init__(self, fruitbak):
		self.lock = Lock()
		self._fruitbak = fruitbak
		self._fds = {}
		self._devs = {}
		self._retired_fds = set()
		self._stderr = open(dup(stderr.fileno()), 'w')
		super().__init__()

	@locked
	@initializer
	def _agent(self):
		return self._fruitbak.pool.agent()

	# latin-1 is an encoding that provides a (dummy) 1:1 byte:char mapping
	encoding = 'latin-1'

	def _fusepy_to_unicode(self, s):
		return s.encode(self.encoding).decode('UTF-8', 'surrogateescape')

	def _unicode_to_fusepy(self, s):
		return s.encode('UTF-8', 'surrogateescape').decode(self.encoding)

	def _log(self, *args):
		print(*args, file = self._stderr, flush = True)

	def _trace(self, function, *args):
		self._log(f'{function}({", ".join(map(repr, args))})')

	_next_fd = 0

	def _allocate_fd(self, obj):
		retired_fds = self._retired_fds
		try:
			fd = retired_fds.pop()
		except KeyError:
			with self.lock:
				fd = self._next_fd
				self._next_fd = fd + 1
		self._fds[fd] = obj
		return fd

	def _deallocate_fd(self, fd):
		del self._fds[fd]
		self._retired_fds.add(fd)

	_next_dev = 0

	def _dev(self, share):
		backup = share.backup
		host = backup.host
		key = host.name, backup.index, share.name
		try:
			return self._devs[key]
		except KeyError:
			pass
		dev = self._next_dev
		self._next_dev = dev + 1
		self._devs[key] = dev
		return dev

	def _parse_path(self, path, root_func, host_func, backup_func, share_func):
		relpath = path.lstrip('/')
		components = relpath.split('/', 3) if relpath else []
		depth = len(components)

		try:
			if depth == 0:
				if root_func is None:
					raise FuseOSError(ENOENT)
				return root_func()

			host = self._fruitbak[self._fusepy_to_unicode(components[0])]
			if depth == 1:
				if host_func is None:
					raise FuseOSError(ENOENT)
				return host_func(host)

			backup = host[int(components[1])]
			if depth == 2:
				if backup_func is None:
					raise FuseOSError(ENOENT)
				return backup_func(host, backup)

			if share_func is None:
				raise FuseOSError(ENOENT)

			share = backup[self._fusepy_to_unicode(components[2])]
			path = components[3].encode(self.encoding) if depth > 3 else b''
			return share_func(host, backup, share, path)
		except (KeyError, FileNotFoundError):
			raise FuseOSError(ENOENT)

	def _open_share(self, host, backup, share, path):
		return self._allocate_fd(FruitFuseFile(dentry = share[path]))

	@windshield
	def open(self, path, flags):
		return self._parse_path(path, None, None, None, self._open_share)

	@windshield
	def read(self, path, size, offset, fd):
		file = self._fds[fd]
		dentry = file.dentry
		hashes = dentry.hashes
		num_hashes = len(hashes)
		file_size = dentry.size

		fbak = self._fruitbak
		chunk_size = fbak.chunk_size
		agent = self._agent

		result = []

		while size and offset < file_size:
			chunk_index, chunk_offset = divmod(offset, chunk_size)
			if file.chunk_index == chunk_index:
				chunk = file.chunk
			elif chunk_index < num_hashes:
				chunk = agent.get_chunk(hashes[chunk_index])
				file.chunk = chunk
				file.chunk_index = chunk_index
			else:
				break
			piece = chunk[chunk_offset : chunk_offset + size]
			piece_len = len(piece)
			offset += piece_len
			size -= piece_len
			result.append(piece)

		return b''.join(result)

	@windshield
	def release(self, path, fd):
		self._deallocate_fd(fd)

	def _dentry2stat(self, share, dentry):
		return dict(
			st_mode = dentry.mode,
			st_atime = dentry.mtime,
			st_ctime = dentry.mtime,
			st_mtime = dentry.mtime,
			st_size = dentry.size,
			st_uid = dentry.uid,
			st_gid = dentry.gid,
			st_ino = (self._dev(share) << 32) + dentry.inode,
		)

	def _getattr_root(self):
		return dict(st_mode = S_IFDIR | 0o555, st_nlink = 2)

	def _getattr_host(self, host):
		try:
			last_backup = host[-1]
		except:
			return dict(st_mode = S_IFDIR | 0o555, st_nlink = 2)
		else:
			return self._getattr_backup(host, last_backup)

	def _getattr_backup(self, host, backup):
		return dict(st_mode = S_IFDIR | 0o555, st_nlink = 2, st_mtime = backup.start_time)

	def _getattr_share(self, host, backup, share, path):
		return self._dentry2stat(share, share[path])

	@windshield
	def getattr(self, path, fd = None):
		return self._parse_path(path, self._getattr_root, self._getattr_host, self._getattr_backup, self._getattr_share)

	@windshield
	def _readlink_share(self, host, backup, share, path):
		return str(share[path].symlink, self.encoding)

	@windshield
	def readlink(self, path):
		return self._parse_path(path, None, None, None, self._readlink_share)

	def _readdir_root(self):
		return (bytes(host.hostdir).decode(self.encoding) for host in self._fruitbak)

	def _readdir_host(self, host):
		return (str(backup.index) for backup in host)

	def _readdir_backup(self, host, backup):
		return (bytes(share.sharedir).decode(self.encoding) for share in backup)

	def _readdir_share(self, host, backup, share, path):
		return (
			(
				dentry.name.split(b'/')[-1].decode(self.encoding),
				self._dentry2stat(share, dentry),
				0,
			) for dentry in share.ls(path)
		)

	@windshield
	def readdir(self, path, fd):
		entries = self._parse_path(path, self._readdir_root, self._readdir_host, self._readdir_backup, self._readdir_share)
		return ['.', '..', *entries]

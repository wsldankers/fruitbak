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

class FruitFuseDirectory(Initializer):
	entries = ('.', '..')

def traced(f):
	name = f.__name__
	@wraps(f)
	def traced(self, *args, **kwargs):
		try:
			self.trace(name, *args)
			return f(self, *args, **kwargs)
		except FuseOSError:
			raise
		except:
			print_exc(file = self.stderr)
		raise FuseOSError(ENOENT)
	return traced

class FruitFuse(FuseOperations):
	use_ns = True

	def __init__(self, fruitbak):
		self.fruitbak = fruitbak
		self.lock = Lock()
		self.fds = dict()
		self.retired_fds = set()
		self.stderr = open(dup(stderr.fileno()), 'w')
		super().__init__()

	@locked
	@initializer
	def fruitbak(self):
		return initialize_fruitbak()

	# latin-1 is an encoding that provides a (dummy) 1:1 byte:char mapping
	encoding = 'latin-1'

	def fusepy_to_unicode(self, s):
		return s.encode(self.encoding).decode('UTF-8', 'surrogateescape')

	def unicode_to_fusepy(self, s):
		return s.encode('UTF-8', 'surrogateescape').decode(self.encoding)

	def log(self, *args):
		print(*args, file = self.stderr, flush = True)

	def trace(self, function, *args):
		self.log(f'{function}({", ".join(map(repr, args))})')

	next_fd = 0

	def allocate_fd(self, obj):
		retired_fds = self.retired_fds
		try:
			fd = retired_fds.pop()
		except KeyError:
			with self.lock:
				fd = self.next_fd
				self.next_fd = fd + 1
		self.fds[fd] = obj
		return fd

	def deallocate_fd(self, fd):
		del self.fds[fd]
		self.retired_fds.add(fd)

	def parse_path(self, path, root_func, host_func, backup_func, share_func):
		relpath = path.lstrip('/')
		components = relpath.split('/', 3) if relpath else []
		depth = len(components)

		try:
			if depth == 0:
				if root_func is None:
					raise FuseOSError(ENOENT)
				return root_func()

			host = self.fruitbak[self.fusepy_to_unicode(components[0])]
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

			share = backup[self.fusepy_to_unicode(components[2])]
			path = components[3].encode(self.encoding) if depth > 3 else b''
			return share_func(host, backup, share, path)
		except (KeyError, FileNotFoundError):
			raise FuseOSError(ENOENT)

	@traced
	def read(self, path, size, offset, fh):
		return self.fds[fh][offset:offset+size]

	def getattr_root(self):
		return dict(st_mode = S_IFDIR | 0o555, st_nlink = 2)

	def getattr_host(self, host):
		try:
			last_backup = host[-1]
		except:
			return dict(st_mode = S_IFDIR | 0o555, st_nlink = 2)
		else:
			return self.getattr_backup(host, last_backup)

	def getattr_backup(self, host, backup):
		return dict(st_mode = S_IFDIR | 0o555, st_nlink = 2, st_mtime = backup.start_time)

	def getattr_share(self, host, backup, share, path):
		try:
			dentry = share[path]
		except (KeyError, FileNotFoundError):
			raise FuseOSError(ENOENT)
		return dict(st_mode = dentry.mode, st_mtime = dentry.mtime, st_size = dentry.size, st_uid = dentry.uid, st_gid = dentry.gid)

	@traced
	def getattr(self, path, fh = None):
		return self.parse_path(path, self.getattr_root, self.getattr_host, self.getattr_backup, self.getattr_share)

	@traced
	def readlink(self, path):
		components = path.lstrip('/').split('/', 3)
		#self.log('components =', repr(components))
		if len(components) < 4:
			raise FuseOSError(ENOENT)
		host, backup, share, path = components
		dentry = self.fruitbak[host][int(backup)][share].get(path.encode(self.encoding))
		return str(dentry.symlink, 'UTF-8', 'surrogateescape')

	@traced
	def open(self, path, flags):
		return self.allocate_fd(path.encode() + b"\n")

	@traced
	def release(self, path, fh):
		self.deallocate_fd(fh)

	def readdir_root(self):
		return (unicode_to_fusepy(host.name) for host in fbak)

	def readdir_host(self, host):
		return (str(backup.index) for backup in host)

	def readdir_backup(self, host, backup):
		return (unicode_to_fusepy(str(share.sharedir)) for share in backup)

	def readdir_share(self, host, backup, share, path):
		return (dentry.name.split(b'/')[-1].decode(self.encoding) for dentry in share.ls(path))

	@traced
	def readdir(self, path, fh):
		entries = self.parse_path(path, self.readdir_root, self.readdir_host, self.readdir_backup, self.readdir_share)
		return ['.', '..', *entries]

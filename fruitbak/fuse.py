from fruitbak.util import Initializer, initializer, locked, fd
from stat import S_IFREG, S_IFDIR
from fusepy import FuseOSError, Operations as FuseOperations
from errno import ENOENT
from threading import Lock
from os import dup
from sys import stderr

class FruitFuseFile(Initializer):
	dentry = None
	chunk = None
	chunk_index = None

class FruitFuse(FuseOperations):
	use_ns = True

	# latin-1 is an encoding that provides a (dummy) 1:1 byte:char mapping
	encoding = 'latin-1'

	def __init__(self):
		self.lock = Lock()
		self.fds = dict()
		self.retired_fds = set()
		self.stderr = open(dup(stderr.fileno()), 'w')

	@locked
	@initializer
	def fruitbak(self):
		return initialize_fruitbak()

	fd = 0

	@locked
	def store_in_fd(self, obj):
		retired_fds = self.retired_fds
		try:
			fd = retired_fds.pop()
		except KeyError:
			fd = self.fd + 1
			self.fd = fd
		self.fds[fd] = obj
		return fd

	def read(self, path, size, offset, fh):
		print('read', repr(path), repr(size), repr(offset), repr(fh), file = stderr, flush = True)
		return self.fds[fh][offset:offset+size]

	def getattr(self, path, fh = None):
		try:
			#print(f'getattr({path!r}, {fh!r})', file = stderr, flush = True)
			components = path.lstrip('/').split('/', 3)
			#print('components', repr(components), file = stderr, flush = True)
			if len(components) == 1:
				host, = components
				if host:
					backup = self.fruitbak[host][-1]
					return dict(st_mode = S_IFDIR | 0o555, st_nlink = 2, st_mtime = backup.start_time)
				else:
					return dict(st_mode = S_IFDIR | 0o555, st_nlink = 2)
			if len(components) == 2:
				host, backup = components
				backup = self.fruitbak[host][int(backup)]
				return dict(st_mode = S_IFDIR | 0o555, st_nlink = 2, st_mtime = backup.start_time)
			if len(components) == 3:
				components.append('')
			host, backup, share, path = components
			dentry = self.fruitbak[host][int(backup)][share].get(path.encode(self.encoding))
			return dict(st_mode = dentry.mode, st_mtime = dentry.mtime, st_size = dentry.size, st_uid = dentry.uid, st_gid = dentry.gid)
		except:
			#print_exc(file = stderr)
			raise FuseOSError(ENOENT)

	def readlink(self, path):
		try:
			#print('getattr', repr(path), repr(fh), file = stderr, flush = True)
			components = path.lstrip('/').split('/', 3)
			#print('components', repr(components), file = stderr, flush = True)
			if len(components) < 4:
				raise FuseOSError(ENOENT)
			host, backup, share, path = components
			dentry = self.fruitbak[host][int(backup)][share].get(path.encode(self.encoding))
			return str(dentry.symlink, 'UTF-8', 'surrogateescape')
		except:
			print_exc(file = stderr)

	def open(self, path, flags):
		print('open', repr(path), repr(flags), file = stderr, flush = True)
		return self.store_in_fd(path.encode() + b"\n")

	def opendir(self, path):
		print('opendir', repr(path), file = stderr, flush = True)
		raise FuseOSError(ENOENT)

	def release(self, fh):
		print('release', repr(fh), file = stderr, flush = True)
		del self.fds[fh]
		self.retired_fds.add(fh)

	def readdir(self, path, fh):
		try:
			#print('readdir', repr(path), repr(fh), file = stderr, flush = True)
			relpath = path.lstrip('/')
			components = relpath.split('/', 4) if relpath else []
			#print('components', repr(components), file = stderr, flush = True)
			depth = len(components)

			fbak = self.fruitbak
			if depth == 0:
				entries = (host.name for host in fbak)
			else:
				host = fbak[components[0]]
				if depth == 1:
					entries = (str(backup.index) for backup in host)
				else:
					backup = host[int(components[1])]
					if depth == 2:
						entries = (str(share.sharedir) for share in backup)
					else:
						sharedir = components[2]
						share, = (share for share in backup if str(share.sharedir) == sharedir)
						sharepath = components[3] if depth > 3 else ''
						entries = (dentry.name.decode().split('/')[-1] for dentry in share.ls(sharepath))

			ret = ['.', '..', *entries]
			#print('entries', repr(ret), file = stderr, flush = True)
			return ret
		except:
			print_exc(file = stderr)

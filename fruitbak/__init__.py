"""Top-level object for a Fruitbak installation"""

from fruitbak.util import Clarity, initializer, sysopendir
from fruitbak.host import Host
from fruitbak.pool import Pool
from fruitbak.config import Config, configurable, configurable_function

from hashset import Hashset

from weakref import WeakValueDictionary
from pathlib import Path
from urllib.parse import quote, unquote
from sys import stderr
from os import getenv, getpid, scandir, rename, unlink
from threading import get_ident as gettid
from concurrent.futures import ThreadPoolExecutor
from itertools import chain

class Fruitbak(Clarity):
	"""Top-level object for a Fruitbak installation.

	For each Fruitbak installation you should instantiate a Fruitbak
	object as a starting point for accessing its configuration and
	backed up hosts.
	"""

	@initializer
	def config(self):
		return Config('global', dir_fd = self.confdir_fd)

	@initializer
	def confdir(self):
		CONF = getenv('FRUITBAK_CONF')
		if CONF is not None:
			return Path(CONF)
		return Path('conf')

	@initializer
	def confdir_fd(self):
		if 'rootdir' in self.__dict__:
			return sysopendir(self.confdir, dir_fd = self.rootdir_fd)
		else:
			return sysopendir(self.confdir)

	@configurable
	def rootdir(self):
		assert False
		for envvar in ('FRUITBAK_ROOT', 'HOME'):
			dir = getenv(envvar)
			if dir is not None:
				return dir
		raise RuntimeError("$HOME not set")

	@rootdir.prepare
	def rootdir(self, value):
		return Path(value)

	@initializer
	def rootdir_fd(self):
		return sysopendir(self.rootdir)

	@configurable
	def hostdir(self):
		return 'host'

	@hostdir.prepare
	def hostdir(self, value):
		return Path(value)

	@initializer
	def hostdir_fd(self):
		return sysopendir(self.hostdir, dir_fd = self.rootdir_fd)

	max_workers = 32

	@initializer
	def executor(self):
		return ThreadPoolExecutor(max_workers = self.max_workers)

	@configurable
	def hashalgo(data):
		from hashlib import sha256
		return sha256

	@initializer
	def hashfunc(self):
		hashalgo = self.hashalgo
		def hashfunc(data):
			h = hashalgo()
			h.update(data)
			return h.digest()
		return hashfunc

	@initializer
	def hashsize(self):
		return len(self.hashfunc(b''))

	@configurable
	def chunksize(self):
		return 2 ** 21

	@chunksize.validate
	def chunksize(self, value):
		if value & value - 1:
			raise RuntimeError("chunksize must be a power of two")
		return int(value)

	def generate_hashes(self):
		pmap = self.executor.map

		# iterate over self, which gives a list of hosts
		# then apply tuple() to each host, giving lists of backups
		# then chain to concatenate those tuples
		backups = chain(*pmap(tuple, self))

		hashes = pmap(lambda s: s.hashes, backups)

		rootdir_fd = self.rootdir_fd

		tempname = 'hashes.%d.%d' % (getpid(), gettid())
		try:
			Hashset.merge(*hashes, path = tempname, dir_fd = rootdir_fd)
			rename(tempname, 'hashes', src_dir_fd = rootdir_fd, dst_dir_fd = rootdir_fd)
		except:
			try:
				unlink(tempname, dir_fd = rootdir_fd)
			except FileNotFoundError:
				pass
		return Hashset.load('hashes', self.hashsize, dir_fd = rootdir_fd)

	@initializer
	def stale_hashes(self):
		try:
			return Hashset.load('hashes', self.hashsize, dir_fd = self.rootdir_fd)
		except FileNotFoundError:
			return self.generate_hashes()

	@initializer
	def pool(self):
		return Pool(fruitbak = self)

	def discover_hosts(self):
		hostcache = self.hostcache
		path_to_name = self.path_to_name

		hosts = {}

		for entry in self.hostdir_fd.scandir():
			entry_name = entry.name
			if not entry_name.startswith('.') and entry.is_dir():
				name = path_to_name(entry_name)
				host = hostcache.get(name)
				if host is None:
					host = Host(fruitbak = self, name = name, backupdir = Path(entry_name))
					hostcache[name] = host
				hosts[name] = host

		with sysopendir('host', dir_fd = self.confdir_fd) as hostconfdir_fd:
			for entry in hostconfdir_fd.scandir():
				entry_name = entry.name
				if not entry_name.startswith('.') and entry_name.endswith('.py') and entry.is_file():
					name = path_to_name(entry_name[:-3])
					if name in hosts:
						continue
					host = hostcache.get(name)
					if host is None:
						host = Host(fruitbak = self, name = name)
						hostcache[name] = host
					hosts[name] = host

		return hosts

	@initializer
	def hostcache(self):
		return WeakValueDictionary()

	def __iter__(self):
		hosts = list(self.discover_hosts().values())
		hosts.sort(key = lambda h: h.name)
		return iter(hosts)

	def __getitem__(self, name):
		hosts = self.discover_hosts()
		return hosts[name]

	def name_to_path(self, name):
		return Path(quote(name[0], errors = 'strict', safe = '+=_,%@')
			+ quote(name[1:], errors = 'strict', safe = '+=_,%@.-'))

	def path_to_name(self, path):
		return unquote(str(path), errors = 'strict')

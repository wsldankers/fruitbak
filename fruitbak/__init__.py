"""Top-level object for a Fruitbak installation"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.sysopen import sysopendir
from fruitbak.host import Host
from fruitbak.config import Config, configurable, configurable_function
from fruitbak.pool import Pool

from weakref import WeakValueDictionary
from pathlib import Path
from urllib.parse import quote, unquote
from sys import stderr
from os import getenv, scandir

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
		return sysopendir(self.confdir, dir_fd = self.rootdir_fd)

	@initializer
	def rootdir(self):
		dir = getenv('FRUITBAK_ROOT')
		if dir is None:
			dir = getenv('HOME')
			if dir is None:
				raise RuntimeError("$HOME not set")
		self.rootdir = dir
		config = self.config
		try:
			dir = config['rootdir']
		except KeyError:
			pass
		return Path(dir)

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

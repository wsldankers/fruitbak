"""Top-level object for a Fruitbak installation"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.weak import weakproperty
from fruitbak.host import Host
from fruitbak.config import Config, configurable, configurable_function
from fruitbak.pool import Pool

from weakref import WeakValueDictionary
from pathlib import Path
from urllib.parse import quote, unquote
from sys import stderr
from os import getenv

class Fruitbak(Clarity):
	"""Top-level object for a Fruitbak installation.

	For each Fruitbak installation you should instantiate a Fruitbak
	object as a starting point for accessing its configuration and
	backed up hosts.
	"""

	@initializer
	def config(self):
		return Config(self.confdir, 'global')

	@configurable
	def confdir(self):
		return self.rootdir / 'conf'

	@confdir.validate
	def confdir(self, value):
		return Path(value)

	@configurable
	def rootdir(self):
		return getenv('HOME')

	@rootdir.prepare
	def rootdir(self, value):
		return Path(value)

	@configurable
	def hostdir(self):
		return self.rootdir / 'host'

	@hostdir.prepare
	def hostdir(self, value):
		return Path(value)

	@configurable
	def pooldir(self):
		return self.rootdir / 'pool'

	@pooldir.prepare
	def pooldir(self, value):
		return Path(value)

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
		return self.hashfunc(b'').length

	#@weakproperty
	@initializer
	def pool(self):
		return Pool(fbak = self, config = {'pooldir': self.pooldir})

	@initializer
	def hostcache(self):
		return WeakValueDictionary()

	def __iter__(self):
		hosts = []
		hostcache = self.hostcache
		for entry in self.hostdir.iterdir():
			entry_name = entry.name
			if not entry_name.startswith('.') and entry.is_dir():
				name = self.path_to_name(entry_name)
				host = hostcache.get(name)
				if host is None:
					host = Host(fruitbak = self, name = name, hostdir = entry)
					hostcache[name] = host
				hosts.append(host)
		return iter(sorted(hosts, key = lambda h: h.name))

	def __getitem__(self, name):
		name = str(name)
		hostcache = self.hostcache
		host = hostcache.get(name)
		if host is None:
			host = Host(fruitbak = self, name = name)
			hostcache[name] = host
		return host

	def name_to_path(self, name):
		return Path(quote(name[0], errors = 'strict', safe = '+=_,%@')
			+ quote(name[1:], errors = 'strict', safe = '+=_,%@.-'))

	def path_to_name(self, path):
		return unquote(str(path), errors = 'strict')

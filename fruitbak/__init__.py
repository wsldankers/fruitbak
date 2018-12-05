"""Top-level object for a Fruitbak installation"""

from fruitbak.util import Initializer, initializer, sysopendir, lockingclass, unlocked
from fruitbak.host import Host
from fruitbak.pool import Pool
from fruitbak.config import Config, configurable, configurable_function

from hashset import Hashset

from weakref import WeakValueDictionary
from pathlib import Path
from urllib.parse import quote, unquote
from sys import stderr
from os import getenv, getpid, sched_getaffinity
from threading import get_ident as gettid, Lock
from concurrent.futures import ThreadPoolExecutor
from itertools import chain

@lockingclass
class Fruitbak(Initializer):
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
			return self.rootdir_fd.sysopendir(self.confdir)
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
		return self.rootdir_fd.sysopendir(self.hostdir)

	@configurable
	def max_parallel_backups(self):
		return 1

	@max_parallel_backups.validate
	def max_parallel_backups(self, value):
		intvalue = int(value)
		if intvalue != value or value < 1:
			raise RuntimeError("max_parallel_backups must be a strictly positive integer")
		return intvalue

	@configurable
	def max_workers(self):
		return 32

	@max_workers.validate
	def max_workers(self, value):
		intvalue = int(value)
		if intvalue != value or value < 1:
			raise RuntimeError("max_workers must be a strictly positive integer")
		return intvalue

	@initializer
	def executor(self):
		return ThreadPoolExecutor(max_workers = self.max_workers)

	@initializer
	def cpu_executor(self):
		return ThreadPoolExecutor(max_workers = len(sched_getaffinity(0)))

	@configurable
	def hash_algo(data):
		from hashlib import sha256
		return sha256

	@initializer
	def hashfunc(self):
		hash_algo = self.hash_algo
		def hashfunc(data):
			h = hash_algo()
			h.update(data)
			return h.digest()
		return hashfunc

	@initializer
	def hash_size(self):
		return len(self.hashfunc(b''))

	@configurable
	def chunk_size(self):
		return 2 ** 21

	@chunk_size.validate
	def chunk_size(self, value):
		if value & value - 1:
			raise RuntimeError("chunk_size must be a power of two")
		return int(value)

	@unlocked
	def generate_hashes(self):
		pmap = self.executor.map

		# iterate over self, which gives a list of hosts
		# then apply tuple() to each host, giving lists of backups
		# then chain to concatenate those tuples
		hashes = pmap(lambda s: s.hashes, chain(*pmap(tuple, self)))

		rootdir_fd = self.rootdir_fd

		tempname = 'hashes.%d.%d' % (getpid(), gettid())
		try:
			Hashset.merge(*hashes, path = tempname, dir_fd = rootdir_fd)
			rootdir_fd.rename(tempname, 'hashes')
		except:
			try:
				rootdir_fd.unlink(tempname)
			except FileNotFoundError:
				pass
		return Hashset.load('hashes', self.hash_size, dir_fd = rootdir_fd)

	@initializer
	def stale_hashes(self):
		try:
			return Hashset.load('hashes', self.hash_size, dir_fd = self.rootdir_fd)
		except FileNotFoundError:
			return self.generate_hashes()

	def remove_hashes(self):
		try:
			del self.stale_hashes
		except AttributeError:
			pass
		try:
			self.rootdir_fd.unlink('hashes')
		except FileNotFoundError:
			pass

	@initializer
	def pool(self):
		return Pool(fruitbak = self)

	@initializer
	def _hostcache(self):
		return WeakValueDictionary()

	@unlocked
	def _discover_hosts(self):
		hostcache = self._hostcache
		path_to_name = self.path_to_name

		hosts = {}

		for entry in self.hostdir_fd.scandir():
			entry_name = entry.name
			if not entry_name.startswith('.') and entry.is_dir():
				name = path_to_name(entry_name)
				with self.lock:
					host = hostcache.get(name)
					if host is None:
						host = Host(fruitbak = self, name = name, backupdir = Path(entry_name))
						hostcache[name] = host
				hosts[name] = host

		with self.confdir_fd.sysopendir('host') as hostconfdir_fd:
			for entry in hostconfdir_fd.scandir():
				entry_name = entry.name
				if not entry_name.startswith('.') and entry_name.endswith('.py') and entry.is_file():
					name = path_to_name(entry_name[:-3])
					if name in hosts:
						continue
					with self.lock:
						host = hostcache.get(name)
						if host is None:
							host = Host(fruitbak = self, name = name)
							hostcache[name] = host
					hosts[name] = host

		return hosts

	@unlocked
	def __iter__(self):
		hosts = list(self._discover_hosts().values())
		hosts.sort(key = lambda h: h.name)
		return iter(hosts)

	@unlocked
	def __getitem__(self, name):
		try:
			return self._hostcache[name]
		except KeyError:
			pass
		hosts = self._discover_hosts()
		return hosts[name]

	@unlocked
	def name_to_path(self, name):
		return Path(quote(name[0], errors = 'strict', safe = '+=_,%@')
			+ quote(name[1:], errors = 'strict', safe = '+=_,%@.-'))

	@unlocked
	def path_to_name(self, path):
		return unquote(str(path), errors = 'strict')

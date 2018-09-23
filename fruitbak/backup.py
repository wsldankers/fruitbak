"""Represent a backup"""

from fruitbak.util import Clarity, initializer, sysopendir, opener
from fruitbak.share import Share

from hardhat import normalize as hardhat_normalize
from hashset import Hashset

from json import load as load_json
from weakref import WeakValueDictionary
from os import fsencode, rename
from pathlib import Path

class Backup(Clarity):
	"""Represent a finished backup.

	As time goes by hosts accrue backups. This class represents
	one of these backups.

	Backups have "shares" (usually filesystems/mountpoints for Unix
	systems and drives for Windows systems) though it's perfectly
	possible to have only one "share" for everything in the backup
	if the distinction is not relevant/applicable for the host.
	"""

	@initializer
	def fruitbak(self):
		"""The fruitbak object that this backup belongs to"""
		return self.host.fruitbak

	@initializer
	def index(self):
		return int(self.backupdir.name)

	@initializer
	def backupdir(self):
		return Path(str(self.index))

	@initializer
	def backupdir_fd(self):
		return sysopendir(self.backupdir, dir_fd = self.host.hostdir_fd)

	@initializer
	def sharedir(self):
		return Path('share')

	@initializer
	def sharedir_fd(self):
		return sysopendir(self.sharedir, dir_fd = self.backupdir_fd)

	@initializer
	def sharecache(self):
		return WeakValueDictionary()

	@initializer
	def hashes(self):
		backupdir_fd = self.backupdir_fd
		hashsize = self.fruitbak.hashsize
		try:
			return Hashset.load('hashes', hashsize, dir_fd = backupdir_fd)
		except FileNotFoundError:
			with open('hashes.new', 'wb', opener = opener(dir_fd = backupdir_fd)) as fp:
				for share in self:
					for blob in share.hashes():
						fp.write(blob)
			Hashset.sortfile('hashes.new', hashsize, dir_fd = backupdir_fd)
			rename('hashes.new', 'hashes', src_dir_fd = backupdir_fd, dst_dir_fd = backupdir_fd)
			return Hashset.load('hashes', hashsize, dir_fd = backupdir_fd)

	@initializer
	def info(self):
		with open('info.json', 'r', opener = opener(dir_fd = self.backupdir_fd)) as fp:
			return load_json(fp)

	@initializer
	def start_time(self):
		t = int(self.info['startTime'])
		if t < 1000000000000000000:
			return t * 1000000000
		else:
			return t

	@initializer
	def end_time(self):
		t = int(self.info['endTime'])
		if t < 1000000000000000000:
			return t * 1000000000
		else:
			return t

	@initializer
	def level(self):
		return int(self.info['level'])

	@initializer
	def failed(self):
		return bool(self.info.get('failed', False))

	def locate_path(self, path):
		original_path = path
		try:
			encode = path.encode
		except AttributeError:
			pass
		else:
			path = encode(errors = 'surrogateescape')
		#print(repr(path))
		path = hardhat_normalize(path)
		path = path.split(b'/') if len(path) else []
		path_len = len(path)
		shares = tuple(self)
		best = None
		best_mp = None
		best_len = -1
		for share in shares:
			mp = hardhat_normalize(share.mountpoint.encode(errors = 'surrogateescape'))
			mp = mp.split(b'/') if len(mp) else []
			mp_len = len(mp)
			#print(best_len, mp_len, path_len, repr(mp), repr(path[:mp_len]))
			if best_len < mp_len <= path_len and mp == path[:mp_len]:
				best = share
				best_mp = mp
				best_len = mp_len
		if best is None:
			raise FileNotFoundError("no share found for '%s'" % original_path)
		return best, b'/'.join(path[best_len:])

	def __iter__(self):
		shares = []
		sharecache = self.sharecache
		fruitbak = self.fruitbak
		for entry in self.sharedir_fd.scandir():
			entry_name = entry.name
			if not entry_name.startswith('.') and entry.is_dir():
				name = fruitbak.path_to_name(entry_name)
				share = sharecache.get(name)
				if share is None:
					share = Share(fruitbak = fruitbak, backup = self, name = name, sharedir = entry)
					sharecache[name] = share
				shares.append(share)
		shares.sort(key = lambda s: s.name)
		return iter(shares)

	def __getitem__(self, name):
		name = str(name)
		sharecache = self.sharecache
		share = sharecache.get(name)
		if share is None:
			share = Share(backup = self, name = name)
			sharecache[name] = share
		return share

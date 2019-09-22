"""Represent a backup"""

from fruitbak.share import Share
from fruitbak.util import (Initializer, initializer, lockingclass, unlocked, ensure_byteslike, time_ns,
	day_interval, week_interval, month_interval, quarter_interval, year_interval)
from fruitbak.config import configurable_property

from hardhat import normalize as hardhat_normalize
from hashset import Hashset

from json import load as load_json
from weakref import WeakValueDictionary
from os import fsencode, rename, unlink, rmdir, fwalk
from pathlib import Path
from collections import deque

from time import localtime, mktime

@lockingclass
class Backup(Initializer):
	"""Represent a finished backup.

	As time goes by hosts accrue backups. This class represents
	one of these backups.

	Backups have "shares" (usually filesystems/mountpoints for Unix
	systems and drives for Windows systems) though it's perfectly
	possible to have only one "share" for everything in the backup
	if the distinction is not relevant/applicable for the host.
	"""

	@unlocked
	@initializer
	def fruitbak(self):
		"""The fruitbak object that this backup belongs to"""
		return self.host.fruitbak

	@unlocked
	@initializer
	def config(self):
		"""The config object of the host that this backup belongs to"""
		return self.host.config

	@unlocked
	@initializer
	def index(self):
		return int(self.backupdir.name)

	@unlocked
	@initializer
	def backupdir(self):
		return Path(str(self.index))

	@initializer
	def backupdir_fd(self):
		return self.host.hostdir_fd.sysopendir(self.backupdir)

	@unlocked
	@initializer
	def sharedir(self):
		return Path('share')

	@initializer
	def sharedir_fd(self):
		return self.backupdir_fd.sysopendir(self.sharedir)

	@initializer
	def sharecache(self):
		return WeakValueDictionary()

	@initializer
	def hashes(self):
		backupdir_fd = self.backupdir_fd
		hash_size = self.fruitbak.hash_size
		try:
			return Hashset.load('hashes', hash_size, dir_fd = backupdir_fd)
		except FileNotFoundError:
			pass
		with open('hashes.new', 'wb', opener = backupdir_fd.opener) as fp:
			for share in self:
				for blob in share.hashes():
					fp.write(blob)
		Hashset.sortfile('hashes.new', hash_size, dir_fd = backupdir_fd)
		rename('hashes.new', 'hashes', src_dir_fd = backupdir_fd, dst_dir_fd = backupdir_fd)
		return Hashset.load('hashes', hash_size, dir_fd = backupdir_fd)

	@unlocked
	@initializer
	def info(self):
		with open('info.json', 'r', opener = self.backupdir_fd.opener) as fp:
			return load_json(fp)

	@unlocked
	@initializer
	def start_time(self):
		t = int(self.info['startTime'])
		if t < 1000000000000000000:
			return t * 1000000000
		else:
			return t

	@unlocked
	@initializer
	def end_time(self):
		t = int(self.info['endTime'])
		if t < 1000000000000000000:
			return t * 1000000000
		else:
			return t

	@unlocked
	@initializer
	def level(self):
		return int(self.info['level'])

	@unlocked
	@property
	def full(self):
		return self.level == 0

	@unlocked
	@initializer
	def failed(self):
		return bool(self.info.get('failed', False))

	@unlocked
	def remove(self):
		def onerror(exc):
			raise exc

		for root, dirs, files, root_fd in fwalk(dir_fd = self.backupdir_fd, topdown = False, onerror = onerror):
			for name in files:
				unlink(name, dir_fd = root_fd)
			for name in dirs:
				rmdir(name, dir_fd = root_fd)

		rmdir(str(self.backupdir), dir_fd = self.host.hostdir_fd)

	@unlocked
	@property
	def age(self):
		return time_ns() - self.start_time

	@unlocked
	@property
	def age_seconds(self):
		return self.age / 1000000000

	@unlocked
	@property
	def age_minutes(self):
		return self.age / 60000000000

	@unlocked
	@property
	def age_hours(self):
		return self.age / 3600000000000

	@unlocked
	@property
	def age_days(self):
		return day_interval(self.start_time, time_ns())

	@unlocked
	@property
	def age_weeks(self):
		return week_interval(self.start_time, time_ns())

	@unlocked
	@property
	def age_months(self):
		return month_interval(self.start_time, time_ns())

	@unlocked
	@property
	def age_quarters(self):
		return quarter_interval(self.start_time, time_ns())

	@unlocked
	@property
	def age_years(self):
		return year_interval(self.start_time, time_ns())

	@unlocked
	@configurable_property
	def expired(self):
		return self.quarters > 1.0

	@initializer
	def log_tier(self):
		# Abuse the side effect of iterating over all backups (it sets log_tier for
		# all backups). Calling deque() like this is the fastest way to iterate over
		# an iterator and discard all items without storing them in memory like
		# list() would.
		deque(self.host, 0)

		return vars(self)['log_tier']

	@unlocked
	def locate_path(self, path):
		original_path = path
		path = hardhat_normalize(ensure_byteslike(path))
		path = path.split(b'/') if len(path) else []
		path_len = len(path)
		shares = tuple(self)
		best = None
		best_mp = None
		best_len = -1
		for share in shares:
			mp = hardhat_normalize(ensure_byteslike(share.mountpoint))
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

	@unlocked
	def __iter__(self):
		fruitbak = self.fruitbak
		path_to_name = fruitbak.path_to_name

		names = {}

		for entry in self.sharedir_fd.scandir():
			entry_name = entry.name
			if not entry_name.startswith('.') and entry.is_dir():
				names[path_to_name(entry_name)] = Path(entry_name)

		lock = self.lock
		sharecache = self.sharecache
		for name in sorted(names.keys()):
			with self.lock:
				share = sharecache.get(name)
				if share is None:
					share = Share(fruitbak = fruitbak, backup = self, name = name, sharedir = names[name])
					sharecache[name] = share

			yield share

	@unlocked
	def __bool__(self):
		return True

	def __getitem__(self, name):
		name = str(name)
		sharecache = self.sharecache
		share = sharecache.get(name)
		if share is None:
			share = Share(backup = self, name = name)
			try:
				share.sharedir_fd
			except FileNotFoundError:
				raise KeyError(name)
			sharecache[name] = share
		return share

	@unlocked
	def get(self, key, default = None):
		try:
			return self[key]
		except KeyError:
			return default

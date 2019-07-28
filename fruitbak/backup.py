"""Represent a backup"""

from fruitbak.share import Share
from fruitbak.util import Initializer, initializer, lockingclass, unlocked, ensure_byteslike
from fruitbak.config import configurable_function

from hardhat import normalize as hardhat_normalize
from hashset import Hashset

from json import load as load_json
from weakref import WeakValueDictionary
from os import fsencode, rename, unlink, rmdir, fwalk
from pathlib import Path

from time import localtime, mktime, struct_time

try:
	from time import time_ns
except ImportError:
	from time import time
	def time_ns():
		return int(time() * 1000000000.0)

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
	def age_seconds(self):
		return (time_ns() - self.start_time) / 1000000000

	@unlocked
	@property
	def age_minutes(self):
		return (time_ns() - self.start_time) / 60000000000

	@unlocked
	@property
	def age_hours(self):
		return (time_ns() - self.start_time) / 3600000000000

	@unlocked
	@property
	def age_days(self):
		return (time_ns() - self.start_time) / 86400000000000

	@unlocked
	@property
	def age_weeks(self):
		return (time_ns() - self.start_time) / 604800000000000

	@unlocked
	@property
	def age_months(self):
		start_time = self.start_time
		start_timestruct = localtime(start_time // 1000000000)
		start_yearmonth = start_timestruct.tm_year * 12 + start_timestruct.tm_mon
		beginning_of_start_month = int(mktime((
			start_timestruct.tm_year,
			start_timestruct.tm_mon,
			1, 0, 0, 0, 0, 0, -1,
		)) * 1000000000)
		ending_of_start_month = int(mktime((
			start_timestruct.tm_year,
			start_timestruct.tm_mon + 1,
			1, 0, 0, 0, 0, 0, -1,
		)) * 1000000000)
		start_month_ratio = ((start_time - beginning_of_start_month)
			/ (ending_of_start_month - beginning_of_start_month))

		current_time = time_ns()
		current_timestruct = localtime(current_time // 1000000000)
		current_yearmonth = current_timestruct.tm_year * 12 + current_timestruct.tm_mon
		if current_yearmonth == start_yearmonth:
			beginning_of_current_month = beginning_of_start_month
			ending_of_current_month = ending_of_start_month
		else:
			beginning_of_current_month = int(mktime((
				current_timestruct.tm_year,
				current_timestruct.tm_mon,
				1, 0, 0, 0, 0, 0, -1,
			)) * 1000000000)
			ending_of_current_month = int(mktime((
				current_timestruct.tm_year,
				current_timestruct.tm_mon + 1,
				1, 0, 0, 0, 0, 0, -1,
			)) * 1000000000)
		current_month_ratio = ((current_time - beginning_of_current_month)
			/ (ending_of_current_month - beginning_of_current_month))

		return (current_yearmonth - start_yearmonth
			+ current_month_ratio - start_month_ratio)

	@unlocked
	@configurable_function
	def expired(self):
		return self.age_months > 3

	@initializer
	def log_tier(self):
		# abuse the side effect:
		iter(self.host)
		return self.__dict__['log_tier']

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
		shares = []
		sharecache = self.sharecache
		fruitbak = self.fruitbak
		for entry in self.sharedir_fd.scandir():
			entry_name = entry.name
			if not entry_name.startswith('.') and entry.is_dir():
				name = fruitbak.path_to_name(entry_name)
				with self.lock:
					share = sharecache.get(name)
					if share is None:
						share = Share(fruitbak = fruitbak, backup = self, name = name, sharedir = Path(entry.name))
						sharecache[name] = share
				shares.append(share)
		shares.sort(key = lambda s: s.name)
		return iter(shares)

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

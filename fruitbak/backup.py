"""Represent a backup"""

from fruitbak.util import Clarity, initializer
from fruitbak.share import Share

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
		return self.host.hostdir_fd.sysopendir(self.backupdir)

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
		hashsize = self.fruitbak.hashsize
		try:
			return Hashset.load('hashes', hashsize, dir_fd = backupdir_fd)
		except FileNotFoundError:
			with open('hashes.new', 'wb', opener = backupdir_fd.opener) as fp:
				for share in self:
					for blob in share.hashes():
						fp.write(blob)
			Hashset.sortfile('hashes.new', hashsize, dir_fd = backupdir_fd)
			rename('hashes.new', 'hashes', src_dir_fd = backupdir_fd, dst_dir_fd = backupdir_fd)
			return Hashset.load('hashes', hashsize, dir_fd = backupdir_fd)

	@initializer
	def info(self):
		with open('info.json', 'r', opener = self.backupdir_fd.opener) as fp:
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

	def remove(self):
		def onerror(exc):
			raise exc

		for root, dirs, files, root_fd in fwalk(dir_fd = self.backupdir_fd, topdown = False, onerror = onerror):
			for name in files:
				unlink(name, dir_fd = root_fd)
			for name in dirs:
				rmdir(name, dir_fd = root_fd)

		rmdir(str(self.backupdir), dir_fd = self.host.hostdir_fd)

	@property
	def age_seconds(self):
		return self.start_time / 1000000000

	@property
	def age_minutes(self):
		return self.start_time / 60000000000

	@property
	def age_hours(self):
		return self.start_time / 3600000000000

	@property
	def age_days(self):
		return self.start_time / 86400000000000

	@property
	def age_weeks(self):
		return self.start_time / 604800000000000

	@property
	def age_months(self):
		start_time = self.start_time
		start_timestruct = localtime(start_time // 1000000000)
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
		start_month_ratio = (start_time - beginning_of_start_month) \
			/ (ending_of_start_month - beginning_of_start_month)

		current_time = time_ns()
		current_timestruct = localtime(current_time // 1000000000)
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
		current_month_ratio = (current_time - beginning_of_current_month) \
			/ (ending_of_current_month - beginning_of_current_month)

		return (current_timestruct.tm_year * 12 + current_timestruct.tm_mon) \
			- (start_timestruct.tm_year * 12 + start_timestruct.tm_mon) \
			+ current_month_ratio - start_month_ratio

	@property
	def expired(self):
		try:
			configured = self.host.config['expired']
		except KeyError:
			return self.age_months > 3
		else:
			return configured(self)

	@property
	def log_tier(self):
		# abuse the side effect:
		iter(self.host)
		return self.__dict__['log_tier']

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
					share = Share(fruitbak = fruitbak, backup = self, name = name, sharedir = entry.name)
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

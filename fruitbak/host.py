"""Represent hosts to back up"""

from fruitbak.util import Initializer, initializer, lockingclass, unlocked
from fruitbak.config import Config, configurable
from fruitbak.backup import Backup
from fruitbak.new.backup import NewBackup

from os import mkdir, listdir
from weakref import WeakValueDictionary
from pathlib import Path
import re

numbers_re = re.compile('0|[1-9][0-9]*')

def ffs(x):
	"""Calculate the number of trailing zero bits of an int.
	Results for 0 are undefined."""

	return (x & -x).bit_length() - 1

@lockingclass
class Host(Initializer):
	"""Represent hosts to back up.

	These can be either hosts that have been backed up in the past
	or hosts that are configured to be backed up (or both).

	Hosts have "shares" (usually filesystems/mountpoints for Unix
	systems and drives for Windows systems) though it's perfectly
	possible to have only one "share" for the entire host if the
	distinction is not relevant/applicable for the host.
	"""

	@unlocked
	@initializer
	def name(self):
		return self.fruitbak.path_to_name(self.hostdir.name)

	@unlocked
	@initializer
	def hostdir(self):
		return self.fruitbak.name_to_path(self.name)

	@unlocked
	@initializer
	def hostdir_fd(self):
		return self.fruitbak.hostdir_fd.sysopendir(self.hostdir)

	@initializer
	def backupcache(self):
		return WeakValueDictionary()

	@initializer
	def env(self):
		return dict(host = self.name)

	@initializer
	def config(self):
		env = self.env
		dir_fd = self.fruitbak.confdir_fd
		try:
			return Config('common', Path('host') / self.name, env = env, preseed = env, dir_fd = dir_fd)
		except FileNotFoundError:
			return Config('common', env = env, preseed = dict(env, auto = False), dir_fd = dir_fd)

	@unlocked
	@configurable
	def auto(self):
		return True

	@auto.validate
	def auto(self, value):
		return bool(value)

	def backup(self, **kwargs):
		try:
			mkdir(str(self.hostdir), dir_fd = self.fruitbak.hostdir_fd)
		except FileExistsError:
			pass
		NewBackup(host = self, **kwargs).backup()

	@unlocked
	def __iter__(self):
		try:
			hostdir_fd = self.hostdir_fd
		except FileNotFoundError:
			return

		indices = {}
		backupcache = self.backupcache
		for entry in hostdir_fd.scandir():
			entry_name = entry.name
			if numbers_re.match(entry_name) and entry.is_dir():
				indices[int(entry_name)] = Path(entry_name)

		log_tiers = {}
		log_indices = {}
		for index in sorted(indices.keys(), reverse = True):
			if index == 0:
				log_tiers[index] = 0
			else:
				log_tier = ffs(index)
				log_tiers[index] = log_indices.setdefault(log_tier, 0)
				log_indices[log_tier] += 1

		lock = self.lock
		for index in sorted(indices.keys()):
			with lock:
				backup = backupcache.get(index)
				if backup is None:
					backup = Backup(
						host = self,
						index = index,
						log_tier = log_tiers[index],
						backupdir = indices[index],
					)
					backupcache[index] = backup
				else:
					backup.log_tier = log_tiers[index]

			yield backup

	@unlocked
	def __getitem__(self, index):
		index = int(index)
		if index < 0:
			return tuple(self)[index]
		backupcache = self.backupcache
		with self.lock:
			backup = backupcache.get(index)
			if backup is None:
				backup = Backup(host = self, index = index)
				backupcache[index] = backup
		return backup

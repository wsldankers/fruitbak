"""Represent hosts to back up"""

from fruitbak.util import Initializer, initializer
from fruitbak.config import Config, configurable
from fruitbak.backup import Backup
from fruitbak.new.backup import NewBackup

from os import mkdir, listdir
from weakref import WeakValueDictionary
from pathlib import Path
import re

numbers_re = re.compile('0|[1-9][0-9]*')

def ffs(x):
	"""Calculate the number of trailing zero bits of an int"""

	if x <= 0:
		return 0
	chunk = 1

	# Double the speed with which we attempt to find a 1.
	# Stop when we find it.
	while x & ((1 << chunk) - 1) == 0:
		chunk <<= 1

	# Our last attempt found a 1, so apparently that was too much.
	chunk >>= 1

	# Ok, so chunk number of bits are confirmed to be 0. Shift those
	# out so we can focus on the rest.
	x >>= chunk
	total = chunk

	# Now that we adjusted x, we know we could never have this many
	# zeros left (otherwise chunk would be double the value).
	chunk >>= 1

	# Now halve our speed each time.
	# We know we can't have the same number of bits twice in a row
	# because then we would have caught it in the previous iteration.
	while chunk:
		if x & ((1 << chunk) - 1) == 0:
			x >>= chunk
			total += chunk
		chunk >>= 1

	return total

class Host(Initializer):
	"""Represent hosts to back up.

	These can be either hosts that have been backed up in the past
	or hosts that are configured to be backed up (or both).

	Hosts have "shares" (usually filesystems/mountpoints for Unix
	systems and drives for Windows systems) though it's perfectly
	possible to have only one "share" for the entire host if the
	distinction is not relevant/applicable for the host.
	"""

	@initializer
	def name(self):
		return self.fruitbak.path_to_name(self.hostdir.name)

	@initializer
	def hostdir(self):
		return self.fruitbak.name_to_path(self.name)

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
		try:
			return Config('common', Path('host') / self.name, env = env, preseed = env, dir_fd = self.fruitbak.confdir_fd)
		except FileNotFoundError:
			return Config('common', preseed = dict(env, auto = False))

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

	def __iter__(self):
		try:
			hostdir_fd = self.hostdir_fd
		except FileNotFoundError:
			return iter(())

		backups = []
		backupcache = self.backupcache
		for entry in hostdir_fd.scandir():
			entry_name = entry.name
			if numbers_re.match(entry_name) and entry.is_dir():
				index = int(entry_name)
				backup = backupcache.get(index)
				if backup is None:
					backup = Backup(host = self, index = index, backupdir = Path(entry_name))
					backupcache[index] = backup
				backups.append(backup)
		backups.sort(key = lambda b: b.index)
		log_indices = {}
		for backup in reversed(backups):
			index = backup.index
			if index == 0:
				backup.__dict__['log_tier'] = 0
			else:
				log_tier = ffs(index)
				# can't set the attribute directly because it's a property
				backup.__dict__['log_tier'] = log_indices.setdefault(log_tier, 0)
				log_indices[log_tier] += 1
		return iter(backups)

	def __getitem__(self, index):
		index = int(index)
		if index < 0:
			return tuple(self)[index]
		backupcache = self.backupcache
		backup = backupcache.get(index)
		if backup is None:
			backup = Backup(host = self, index = index)
			backupcache[index] = backup
		return backup

"""Represent hosts to back up"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.sysopen import sysopendir
from fruitbak.config import Config
from fruitbak.backup import Backup
from fruitbak.new.backup import NewBackup

from os import mkdir, listdir
from weakref import WeakValueDictionary
from pathlib import Path
import re

numbers_re = re.compile('0|[1-9][0-9]*')

class Host(Clarity):
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
		return sysopendir(self.hostdir, dir_fd = self.fruitbak.hostdir_fd)

	@initializer
	def backupcache(self):
		return WeakValueDictionary()

	@initializer
	def env(self):
		return dict(host = self.name)

	@initializer
	def config(self):
		return Config(Path('host') / self.name, dir_fd = self.fruitbak.confdir_fd)

	def backup(self):
		try:
			mkdir(str(self.hostdir), dir_fd = self.fruitbak.hostdir_fd)
		except FileExistsError:
			pass
		NewBackup(host = self).backup()

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

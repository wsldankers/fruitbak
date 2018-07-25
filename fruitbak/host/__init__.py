"""Represent hosts to back up"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.weak import weakproperty
from fruitbak.backup import Backup
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
	def fruitbak(self): pass

	@initializer
	def name(self):
		return self.fruitbak.path_to_name(self.hostdir.name)

	@initializer
	def hostdir(self):
		path = self.fruitbak.hostdir / self.fruitbak.name_to_path(self.name)
		try:
			path.mkdir(exist_ok = True)
		except FileExistsError:
			pass
		return path

	def backups(self):
		backups = []
		for entry in self.hostdir.iterdir():
			if numbers_re.match(entry.name) and entry.is_dir():
				backups.append(Backup(host = self, backupdir = entry))
		return sorted(backups, key = lambda b: b.index)

	def backup(self, index):
		return Backup(host = self, index = index)

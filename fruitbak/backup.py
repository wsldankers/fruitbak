"""Represent a backup"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.weak import weakproperty
from fruitbak.share import Share

from weakref import WeakValueDictionary

class Backup(Clarity):
	"""Represent a backup.

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
		return self.host.hostdir / str(self.index)

	@initializer
	def sharedir(self):
		return self.backupdir / 'share'

	@initializer
	def sharecache(self):
		return WeakValueDictionary()

	def __iter__(self):
		shares = []
		sharecache = self.sharecache
		fruitbak = self.fruitbak
		for entry in self.sharedir.iterdir():
			entry_name = entry.name
			if not entry_name.startswith('.') and entry.is_dir():
				name = fruitbak.path_to_name(entry_name)
				share = sharecache.get(name)
				if share is None:
					share = Share(fruitbak = fruitbak, backup = self, name = name, sharedir = entry)
					sharecache[name] = share
				shares.append(share)
		return iter(sorted(shares, key = lambda s: s.name))

	def __getitem__(self, name):
		name = str(name)
		sharecache = self.sharecache
		share = sharecache.get(name)
		if share is None:
			share = Share(backup = self, name = name)
			sharecache[name] = share
		return share

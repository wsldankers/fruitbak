"""Represent a backup"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.weak import weakproperty
from fruitbak.share import Share
from hardhat import Hardhat

class Backup(Clarity):
	"""Represent a backup.

	As time goes by hosts accrue backups. This class represents
	one of these backups.

	Backups have "shares" (usually filesystems/mountpoints for Unix
	systems and drives for Windows systems) though it's perfectly
	possible to have only one "share" for everything in the backup
	if the distinction is not relevant/applicable for the host.
	"""

	@weakproperty
	def fruitbak(self):
		"""The fruitbak object that this backup belongs to"""
		return self.host.fruitbak

	@weakproperty
	def host(self):
		"""The host object that this backup belongs to"""

	@initializer
	def index(self):
		return int(self.backupdir.name)

	@initializer
	def backupdir(self):
		path = self.host.sharedir / str(self.index)
		try:
			path.mkdir(exist_ok = True)
		except FileExistsError:
			pass
		return path

	@initializer
	def sharedir(self):
		return self.backupdir / 'share'

	@initializer
	def shares(self):
		shares = []
		for entry in self.sharedir.iterdir():
			if not entry.name.startswith('.') and entry.is_dir():
				shares.append(Share(backup = self, sharedir = entry))
		return sorted(shares, key = lambda s: s.name)

"""Represent hosts to back up"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.weak import weakproperty
from fruitbak.share import Share

class Host(Clarity):
	"""Represent hosts to back up.

	These can be either hosts that have been backed up in the past
	or hosts that are configured to be backed up (or both).

	Hosts have "shares" (usually filesystems/mountpoints for Unix
	systems and drives for Windows systems) though it's perfectly
	possible to have only one "share" for the entire host if the
	distinction is not relevant/applicable for the host.
	"""

	@weakproperty
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

	@initializer
	def sharedir(self):
		return self.hostdir / 'share'

	@initializer
	def shares(self):
		shares = []
		for entry in self.sharedir.iterdir():
			if entry.is_dir() and not entry.name.startswith('.'):
				shares.append(Share(host = self, sharedir = entry))
		return shares

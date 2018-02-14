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
	def hostdir(self):
		return self.fruitbak.hostdir / self.name

	@initializer
	def sharedir(self):
		return self.hostdir / 'share'

	@initializer
	def shares(self):
		for entry in self.sharedir.iterdir():
			if entry.is_dir() and not entry.name.startswith('.'):
				return Share(name = entry.name, host = self)

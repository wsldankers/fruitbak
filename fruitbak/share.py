"""Represent hosts to back up"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.weak import weakproperty

class Share(Clarity):
	"""Represent a share to back up.

	Hosts have "shares" (usually filesystems/mountpoints for Unix
	systems and drives for Windows systems) though it's perfectly
	possible to have only one "share" for the entire host if the
	distinction is not relevant/applicable for the host.
	"""

	@weakproperty
	def fruitbak(self):
		return self.host.fruitbak

	@weakproperty
	def host(self): pass

	@initializer
	def sharedir(self):
		return self.host.sharedir / self.name

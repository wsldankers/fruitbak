"""Represent hosts to back up"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.weak import weakproperty
from hardhat import Hardhat

class Share(Clarity):
	"""Represent a share to back up.

	Hosts have "shares" (usually filesystems/mountpoints for Unix
	systems and drives for Windows systems) though it's perfectly
	possible to have only one "share" for the entire host if the
	distinction is not relevant/applicable for the host.
	"""

	@weakproperty
	def fruitbak(self):
		"""The fruitbak object that this share belongs to"""
		return self.host.fruitbak

	@weakproperty
	def host(self):
		"""The host object that this share belongs to"""

	@initializer
	def name(self):
		return self.fruitbak.path_to_name(self.sharedir.name)

	@initializer
	def sharedir(self):
		path = self.host.sharedir / self.fruitbak.name_to_path(self.name)
		try:
			path.mkdir(exist_ok = True)
		except FileExistsError:
			pass
		return path

	@initializer
	def metadata(self):
		return Hardhat(self.sharedir / 'metadata.hh')

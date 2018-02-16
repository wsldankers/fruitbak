"""Represent hosts to back up"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.weak import weakproperty
from fruitbak.dentry import Dentry, HardlinkDentry
from hardhat import Hardhat

class Share(Clarity):
	"""Represent a share to back up.

	Hosts have "shares" (usually filesystems/mountpoints for Unix
	systems and drives for Windows systems) though it's perfectly
	possible to have only one "share" for the entire host if the
	distinction is not relevant/applicable for the host.
	"""

	MAXNAMELEN = 65535
	FORMAT_FLAG_HARDLINK = 0x1
	FORMAT_MASK = FORMAT_FLAG_HARDLINK

	@weakproperty
	def fruitbak(self):
		"""The fruitbak object that this share belongs to"""
		return self.host.fruitbak

	@weakproperty
	def host(self):
		"""The host object that this share belongs to"""
		return self.backup.host

	@weakproperty
	def backup(self):
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
		return Hardhat(str(self.sharedir / 'metadata.hh'))

	def parse_dentry(path, data):
		(flags, mode, size, mtime, uid, gid) = unpack_from('<LLQQLL', data)
		if flags & FORMAT_FLAG_HARDLINK:
			original = Dentry(is_hardlink = True)
		else:
		

	def ls(self, path):
		for (path, data) in self.metadata.ls(path):
			yield self.parse_dentry(path, data)

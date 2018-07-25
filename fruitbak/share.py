"""Represent hosts to back up"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.weak import weakproperty
from fruitbak.dentry import Dentry, HardlinkDentry
from hardhat import Hardhat
from struct import Struct

class ShareError(Exception):
    """Something Share-related went wrong."""
    pass

class NestedHardlinkError(ShareError):
    pass

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

	dentry_layout = Struct('<LLQQLL')

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

	def parse_dentry(self, path, data):
		dentry_layout = self.dentry_layout
		dentry_layout_size = dentry_layout.size
		FORMAT_FLAG_HARDLINK = self.FORMAT_FLAG_HARDLINK
		MAXNAMELEN = self.MAXNAMELEN
		(flags, mode, size, mtime, uid, gid) = dentry_layout.unpack_from(data)
		if flags & FORMAT_FLAG_HARDLINK:
			hardlink = data[dentry_layout_size:]
			original = Dentry(
				name = path,
				flags = flags,
				mode = mode,
				size = size,
				mtime = mtime,
				uid = uid,
				gid = gid,
				is_hardlink = True,
				hardlink = hardlink,
			)
			data = self.metadata.get(hardlink)
			(flags, mode, size, mtime, uid, gid) = dentry_layout.unpack_from(data)
			if flags & FORMAT_FLAG_HARDLINK:
				raise NestedHardlinkError("'%s' is a hardlink pointing to '%s', but that is also a hardlink" % (name, original.name))
			if len(data) > dentry_layout_size + MAXNAMELEN:
				extra = memoryview(data)[dentry_layout_size:]
			else:
				extra = data[dentry_layout_size:]
			target = Dentry(
				name = hardlink,
				flags = flags,
				mode = mode,
				size = size,
				mtime = mtime,
				uid = uid,
				gid = gid,
				extra = extra,
			)
			return HardlinkDentry(original, target)
		else:
			if len(data) > dentry_layout_size + MAXNAMELEN:
				extra = memoryview(data)[dentry_layout_size:]
			else:
				extra = data[dentry_layout_size:]
			return Dentry(
				name = path,
				flags = flags,
				mode = mode,
				size = size,
				mtime = mtime,
				uid = uid,
				gid = gid,
				extra = extra,
			)
		

	def ls(self, path):
		for (path, data) in self.metadata.ls(path):
			yield self.parse_dentry(path, data)

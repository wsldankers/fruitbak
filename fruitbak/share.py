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

	@initializer
	def fruitbak(self):
		"""The fruitbak object that this share belongs to"""
		return self.host.fruitbak

	@initializer
	def host(self):
		"""The host object that this share belongs to"""
		return self.backup.host

	@initializer
	def name(self):
		return self.fruitbak.path_to_name(self.sharedir.name)

	@initializer
	def sharedir(self):
		path = self.backup.backupdir / 'share' / self.fruitbak.name_to_path(self.name)
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
				extra = hardlink,
				share = self,
			)
			data = self.metadata[hardlink]
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
				share = self,
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
				share = self,
			)

	def ls(self, path = b'', parent = None):
		c = self.metadata.ls(path)
		if parent or parent is None:
			try:
				path, data = c.item
			except KeyError:
				if parent:
					yield None
			else:
				yield self.parse_dentry(path, data)
		for path, data in c:
			yield self.parse_dentry(path, data)

	def find(self, path = b'', parent = None):
		c = self.metadata.find(path)
		if parent or parent is None:
			try:
				path, data = c.item
			except KeyError:
				if parent:
					yield None
			else:
				yield self.parse_dentry(path, data)
		for path, data in c:
			yield self.parse_dentry(path, data)

	def __getitem__(self, path):
		return self.parse_dentry(path, self.metadata[path])

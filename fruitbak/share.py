"""Represent hosts to back up"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.dentry import Dentry, HardlinkDentry, dentry_layout_size

from hardhat import Hardhat

from struct import Struct
from json import load as load_json
from sys import stderr

class ShareError(Exception):
    """Something Share-related went wrong."""
    pass

class NestedHardlinkError(ShareError):
    pass

class MissingLinkError(ShareError):
    pass

class Share(Clarity):
	"""Represent a share to back up.

	Hosts have "shares" (usually filesystems/mountpoints for Unix
	systems and drives for Windows systems) though it's perfectly
	possible to have only one "share" for the entire host if the
	distinction is not relevant/applicable for the host.
	"""

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
		return self.backup.backupdir / 'share' / self.fruitbak.name_to_path(self.name)

	@initializer
	def info(self):
		info_path = self.sharedir / 'info.json'
		with info_path.open('r') as fp:
			return load_json(fp)

	@initializer
	def start_time(self):
		return int(self.info['startTime']) * 1000000000

	@initializer
	def end_time(self):
		return int(self.info['endTime']) * 1000000000

	@initializer
	def mountpoint(self):
		return str(self.info['mountpoint'])

	@initializer
	def path(self):
		return str(self.info['path'])

	@initializer
	def error(self):
		try:
			return str(self.info['error'])
		except KeyError:
			return None

	@initializer
	def metadata(self):
		return Hardhat(str(self.sharedir / 'metadata.hh'))

	def parse_dentry(self, path, data):
		dentry = Dentry(data, name = path, share = self)
		if dentry.is_hardlink:
			name = dentry.hardlink
			target = Dentry(self.metadata[name], name = path, share = self)
			if target.is_hardlink:
				raise NestedHardlinkError("'%s' is a hardlink pointing to '%s', but that is also a hardlink" % (name, original.name))
			return HardlinkDentry(dentry, target)
		else:
			return dentry

	def ls(self, path = b'', parent = False):
		return self.hardlink_inverter(self.metadata.ls(path, parent = parent))

	def find(self, path = b'', parent = True):
		return self.hardlink_inverter(self.metadata.find(path, parent = parent))

	def hardlink_inverter(self, c):
		remap = {}
		first_inode = None
		metadata = self.metadata
		for path, data in c:

			inode = c.inode
			if first_inode is None:
				first_inode = inode

			dentry = Dentry(data, name = path, share = self)

			try:
				remapped = remap[path]
			except KeyError:
				pass
			else:
				# This is a normal entry, but it was the target of a hardlink that
				# was output earlier as if it was a regular, non-hardlink dentry.
				# So now we'll pretend that *this* was the hardlink.

				del remap[path]

				target = Dentry(remapped, share = self)
				remapped_path = target.extra

				target.is_hardlink = False
				target.name = remapped_path
				target.extra = dentry.extra

				dentry.is_hardlink = True
				dentry.hardlink = remapped_path

				yield HardlinkDentry(dentry, target)
				continue

			if dentry.is_hardlink:
				target_cursor = metadata.ls(dentry.hardlink)
				try:
					target_path = target_cursor.key
					target_data = target_cursor.value
					target_inode = target_cursor.inode
				except KeyError as e:
					raise MissingLinkError("'%s' is a hardlink to '%s' but the latter does not exist" % (path, dentry.hardlink)) from e

				target = Dentry(target_data, name = target_path, share = self)
				if target.is_hardlink:
					raise NestedHardlinkError("'%s' is a hardlink pointing to '%s', but that is also a hardlink" % (path, extra))
				target_extra = target.extra

				if first_inode <= target_inode < inode:
					# target is already output
					yield HardlinkDentry(dentry, target)
				else:
					# We'll pretend that this was the original file and output a hardlink later.
					remap[target_path] = b''.join((data[:dentry_layout_size], path))

					target.name = path
					yield target

				continue

			yield dentry

	def __getitem__(self, path):
		return self.parse_dentry(path, self.metadata[path])

"""Represent hosts to back up"""

from fruitbak.dentry import Dentry, HardlinkDentry, dentry_layout_size
from fruitbak.util import Initializer, initializer, lockingclass, unlocked, ensure_byteslike, ensure_str

from hardhat import Hardhat, normalize as hardhat_normalize

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

@lockingclass
class Share(Initializer):
	"""Represent a share to back up.

	Hosts have "shares" (usually filesystems/mountpoints for Unix
	systems and drives for Windows systems) though it's perfectly
	possible to have only one "share" for the entire host if the
	distinction is not relevant/applicable for the host.
	"""

	@unlocked
	@initializer
	def fruitbak(self):
		"""The fruitbak object that this share belongs to"""
		return self.host.fruitbak

	@unlocked
	@initializer
	def host(self):
		"""The host object that this share belongs to"""
		return self.backup.host

	@unlocked
	@initializer
	def name(self):
		return self.fruitbak.path_to_name(self.sharedir.name)

	@unlocked
	@initializer
	def sharedir(self):
		return self.fruitbak.name_to_path(self.name)

	@initializer
	def sharedir_fd(self):
		return self.backup.sharedir_fd.sysopendir(self.sharedir)

	@initializer
	def info(self):
		try:
			return self.backup.info['shares'][self.name]
		except KeyError:
			with open('info.json', 'r', opener = self.sharedir_fd.opener) as fp:
				return load_json(fp)

	@unlocked
	@initializer
	def start_time(self):
		t = int(self.info['startTime'])
		if t < 1000000000000000000:
			return t * 1000000000
		else:
			return t

	@unlocked
	@initializer
	def end_time(self):
		t = int(self.info['endTime'])
		if t < 1000000000000000000:
			return t * 1000000000
		else:
			return t

	@unlocked
	@initializer
	def mountpoint(self):
		return str(self.info['mountpoint'])

	@unlocked
	@initializer
	def path(self):
		return str(self.info['path'])

	@unlocked
	@initializer
	def error(self):
		try:
			return str(self.info['error'])
		except KeyError:
			return None

	@initializer
	def metadata(self):
		return Hardhat('metadata.hh', dir_fd = self.sharedir_fd)

	@unlocked
	def _parse_dentry(self, name, data):
		dentry = Dentry(data, name = name, share = self)
		if dentry.is_hardlink:
			target_name = dentry.hardlink
			target = Dentry(self.metadata[target_name], name = target_name, share = self)
			if target.is_hardlink:
				raise NestedHardlinkError("'%s' is a hardlink pointing to '%s', but that is also a hardlink" % (ensure_str(dentry.name), ensure_str(target_name)))
			return HardlinkDentry(dentry, target)
		else:
			return dentry

	@unlocked
	def hashes(self):
		for data in self.metadata.values():
			d = Dentry(data)
			if d.is_file and not d.is_hardlink:
				yield d.extra

	@unlocked
	def ls(self, path = b'', parent = False, strict = True):
		return self.hardlink_inverter(self.metadata.ls(path, parent = parent), strict)

	@unlocked
	def find(self, path = b'', parent = True, strict = False):
		return self.hardlink_inverter(self.metadata.find(path, parent = parent), strict)

	@unlocked
	def hardlink_inverter(self, c, strict):
		remap = {}
		first_inode = None
		metadata = self.metadata
		for name, data in c:
			inode = c.inode
			if first_inode is None:
				first_inode = inode

			dentry = Dentry(data, name = name, share = self)

			try:
				remapped = remap[name]
			except KeyError:
				pass
			else:
				# This is a normal entry, but it was the target of a hardlink that
				# was output earlier as if it was a regular, non-hardlink dentry.
				# So now we'll pretend that *this* was the hardlink.

				if not strict:
					# Doing this saves memory but may generate a => b => c type hardlink
					# chains for remapped hardlinks. Not pretty but technically still
					# correct for most purposes.
					del remap[name]

				target = Dentry(remapped, share = self)
				remapped_name = target.extra

				target.is_hardlink = False
				target.name = remapped_name
				target.extra = dentry.extra

				dentry.is_hardlink = True
				dentry.hardlink = remapped_name

				yield HardlinkDentry(dentry, target)
				continue

			if dentry.is_hardlink:
				hardlink = dentry.hardlink
				try:
					remapped = remap[hardlink]
				except KeyError:
					pass
				else:
					# This is a hardlink to another entry that got demoted from
					# original hardlink target to just a hardlink. Return a hardlink
					# that points to the new "real" file.

					target = Dentry(metadata[hardlink], share = self)

					remapped_name = remapped[dentry_layout_size:]
					target.name = remapped_name
					dentry.hardlink = remapped_name

					yield HardlinkDentry(dentry, target)
					continue

				target_cursor = metadata.ls(hardlink)
				try:
					target_name = target_cursor.key
					target_data = target_cursor.value
					target_inode = target_cursor.inode
				except KeyError as e:
					raise MissingLinkError("'%s' is a hardlink to '%s' but the latter does not exist" % (ensure_str(name), ensure_str(hardlink))) from e

				target = Dentry(target_data, name = target_name, share = self)
				if target.is_hardlink:
					raise NestedHardlinkError("'%s' is a hardlink pointing to '%s', but that is also a hardlink" % (ensure_str(name), ensure_str(target_name)))

				if first_inode <= target_inode < inode:
					# target is already output
					yield HardlinkDentry(dentry, target)
				else:
					# We'll pretend that this was the original file and output a hardlink later.
					remap[target_name] = b''.join((data[:dentry_layout_size], name))

					target.name = name
					yield target

				continue

			yield dentry

	@unlocked
	def __bool__(self):
		return True

	@unlocked
	def __len__(self):
		return len(self.metadata)

	@unlocked
	def __getitem__(self, path):
		path = hardhat_normalize(ensure_byteslike(path))
		return self._parse_dentry(path, self.metadata[path])

	@unlocked
	def get(self, key, default = None):
		try:
			return self[key]
		except KeyError:
			return default

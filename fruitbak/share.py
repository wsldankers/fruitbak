"""Represent hosts to back up"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.dentry import Dentry, HardlinkDentry

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
		dentry_layout = self.dentry_layout
		dentry_layout_size = dentry_layout.size
		FORMAT_FLAG_HARDLINK = self.FORMAT_FLAG_HARDLINK
		flags, mode, size, mtime, uid, gid = dentry_layout.unpack_from(data)
		if flags & FORMAT_FLAG_HARDLINK:
			hardlink = data[dentry_layout_size:]
			original = Dentry(
				name = path,
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
			flags, mode, size, mtime, uid, gid = dentry_layout.unpack_from(data)
			if flags & FORMAT_FLAG_HARDLINK:
				raise NestedHardlinkError("'%s' is a hardlink pointing to '%s', but that is also a hardlink" % (name, original.name))
			extra = data[dentry_layout_size:]
			target = Dentry(
				name = hardlink,
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
			extra = data[dentry_layout_size:]
			return Dentry(
				name = path,
				mode = mode,
				size = size,
				mtime = mtime,
				uid = uid,
				gid = gid,
				extra = extra,
				share = self,
			)

	def ls(self, path = b'', parent = False):
		return self.hardlink_inverter(self.metadata.ls(path, parent = parent))

	def find(self, path = b'', parent = True):
		return self.hardlink_inverter(self.metadata.find(path, parent = parent))

	def hardlink_inverter(self, c):
		remap = {}
		first_inode = None
		metadata = self.metadata
		for path, data in c:
			dentry_layout = self.dentry_layout
			dentry_layout_size = dentry_layout.size
			FORMAT_FLAG_HARDLINK = self.FORMAT_FLAG_HARDLINK

			inode = c.inode
			if first_inode is None:
				first_inode = inode

			flags, mode, size, mtime, uid, gid = dentry_layout.unpack_from(data)

			extra = data[dentry_layout_size:]

			try:
				remapped = remap[path]
			except KeyError:
				pass
			else:
				# This is a normal entry, but it was the target of a hardlink that
				# was output earlier as if it was a regular, non-hardlink dentry.
				# So now we'll pretend that *this* was the hardlink.

				del remap[path]

				remapped_path = remapped[dentry_layout_size:]

				original = Dentry(
					name = path,
					mode = mode,
					size = size,
					mtime = mtime,
					uid = uid,
					gid = gid,
					is_hardlink = True,
					extra = remapped_path,
					share = self,
				)

				flags, mode, size, mtime, uid, gid = dentry_layout.unpack_from(remapped)
				target = Dentry(
					name = remapped_path,
					mode = mode,
					size = size,
					mtime = mtime,
					uid = uid,
					gid = gid,
					extra = extra,
					share = self,
				)

				yield HardlinkDentry(original, target)
				continue

			if flags & FORMAT_FLAG_HARDLINK:
				target_cursor = metadata.ls(extra)
				try:
					target_path = target_cursor.key
					target_data = target_cursor.value
					target_inode = target_cursor.inode
				except KeyError as e:
					raise MissingLinkError("'%s' is a hardlink to '%s' but the latter does not exist" % (path, extra)) from e

				target_extra = target_data[dentry_layout_size:]

				if first_inode <= target_inode < inode:
					# target is already output

					original = Dentry(
						name = path,
						mode = mode,
						size = size,
						mtime = mtime,
						uid = uid,
						gid = gid,
						is_hardlink = True,
						extra = extra,
						share = self,
					)

					flags, mode, size, mtime, uid, gid = dentry_layout.unpack_from(target_data)
					if flags & FORMAT_FLAG_HARDLINK:
						raise NestedHardlinkError("'%s' is a hardlink pointing to '%s', but that is also a hardlink" % (path, extra))

					target = Dentry(
						name = target_path,
						mode = mode,
						size = size,
						mtime = mtime,
						uid = uid,
						gid = gid,
						extra = target_extra,
						share = self,
					)

					yield HardlinkDentry(original, target)
				else:
					remap[target_path] = b''.join((data[:dentry_layout_size], path))

					yield Dentry(
						name = path,
						mode = mode,
						size = size,
						mtime = mtime,
						uid = uid,
						gid = gid,
						extra = target_extra,
						share = self,
					)

				continue

			yield Dentry(
				name = path,
				mode = mode,
				size = size,
				mtime = mtime,
				uid = uid,
				gid = gid,
				extra = extra,
				share = self,
			)

	def __getitem__(self, path):
		return self.parse_dentry(path, self.metadata[path])

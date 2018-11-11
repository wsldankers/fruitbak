from fruitbak.util import Clarity, initializer, ensure_bytes
from fruitbak.dentry import Dentry
from fruitbak.transfer import Transfer
from fruitbak.config import configurable

from rsync_fetch import RsyncFetch

from pathlib import Path, PurePath
from sys import stderr
from stat import *
from traceback import print_exc
from re import compile as re
from itertools import chain
from os import makedev

rsync_filter_escape_find_re = re(rb'[*?[]')
rsync_filter_escape_replace_re = re(rb'[*?[\\]')

def rsync_filter_escape(path, force = False):
	path = bytes(path)
	if force or rsync_filter_escape_find_re.search(path) is not None:
		return rsync_filter_escape_replace_re.sub(path, r'"\\\1')
	return path

def samedentry(a, b):
	if a is None:
		return False
	if b is None:
		return False
	if a.mode != b.mode:
		return False
	if a.size != b.size:
		return False
	if a.mtime != b.mtime:
		return False
	if a.uid != b.uid:
		return False
	if a.gid != b.gid:
		return False
	return True

class RsyncTransfer(Transfer):
	@initializer
	def filters(self):
		filters = set()
		mountpoint = Path(self.mountpoint)
		for exclude in self.excludes:
			path = Path(exclude)
			if path.is_absolute():
				try:
					path = path.relative_to(mountpoint)
				except ValueError:
					continue
			if exclude.endswith('/') and e != '/':
				filters.add(b'- /' + rsync_filter_escape(path, force = True) + b'/**')
			else:
				filters.add(b'- /' + rsync_filter_escape(path))
		try:
			custom_filters = self.config['filters']
		except KeyError:
			return tuple(filters)
		else:
			return tuple(chain(filters, map(ensure_bytes, custom_filters)))

	def transfer(self):
		newshare = self.newshare
		reference = self.reference
		one_filesystem = (b'--one-file-system',) if self.one_filesystem else ()

		def normalize(path):
			return '/'.join(path.relative_to(path.anchor).parts)

		command = (b'/usr/bin/rsync',) + RsyncFetch.required_options + one_filesystem + (bytes(self.path),)

		def entry_callback(name, size, mtime, mode, uid, user, gid, group, major, minor, symlink, hardlink):
			dentry = Dentry(name = name, size = size, mtime = mtime, mode = mode, uid = uid, gid = gid)
			if hardlink is None:
				if dentry.is_file:
					ref_dentry = reference.get(name)
					if samedentry(dentry, ref_dentry):
						dentry.size = ref_dentry.size
						dentry.hashes = ref_dentry.hashes
					elif size > 0:
						#print(name, size, mtime, mode, uid, user, gid, group, major, minor, symlink, hardlink, file = stderr)
						hashes = []
						size = 0
						def data_callback(chunk = None):
							nonlocal size
							if chunk is None:
								dentry.size = size
								dentry.hashes = hashes
								newshare.add_dentry(dentry)
							else:
								size += len(chunk)
								hashes.append(newshare.put_chunk(chunk))
						return data_callback
				elif dentry.is_symlink:
					dentry.symlink = symlink
				elif dentry.is_device:
					dentry.rdev = major, minor
			else:
				dentry.is_hardlink = True
				dentry.hardlink = hardlink
			newshare.add_dentry(dentry)

		with RsyncFetch(
					command = command,
					entry_callback = entry_callback,
					#error_callback = error_callback,
					filters = self.filters,
					chunk_size = self.fruitbak.chunk_size,
				) as rf:
			rf.run()


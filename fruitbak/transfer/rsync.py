from pathlib import Path, PurePath
from sys import stderr
from stat import *
from traceback import print_exc
from re import compile as re
from os import makedev, environ

from rsync_fetch import RsyncFetch

from fruitbak.util import Initializer, initializer, ensure_bytes, convert_env, merge_env
from fruitbak.dentry import Dentry
from fruitbak.transfer import Transfer
from fruitbak.config import configurable

_rsync_filter_escape_find_re = re(rb'[*?[]')
_rsync_filter_escape_replace_re = re(rb'[*?[\\]')

def _rsync_filter_escape(path, force = False):
	path = ensure_bytes(path)
	if force or _rsync_filter_escape_find_re.search(path) is not None:
		return _rsync_filter_escape_replace_re.sub(rb'\\\1', path)
	return path

def _samedentry(a, b):
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
	@configurable('rsync_command')
	def command(self):
		return '''exec ssh ${port+-p "$port"} ${user+-l "$user"} -- "$host" exec rsync "$@"'''

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
			if exclude.endswith('/') and exclude != '/':
				filters.add(b'- /' + _rsync_filter_escape(path, force = True) + b'/**')
			else:
				filters.add(b'- /' + _rsync_filter_escape(path))
		config = self.config
		try:
			custom_filters = config['rsync_filters']
		except KeyError:
			return tuple(filters)
		else:
			return (*filters, *map(ensure_bytes, custom_filters))

	def transfer(self):
		newshare = self.newshare
		reference = self.reference
		one_filesystem = (b'--one-file-system',) if self.one_filesystem else ()

		def normalize(path):
			return '/'.join(path.relative_to(path.anchor).parts)

		command = (b'/bin/sh', b'-ec', self.command, b'rsync', *RsyncFetch.required_options, *one_filesystem, bytes(self.path))

		def entry_callback(name, size, mtime, mode, uid, user, gid, group, major, minor, symlink, hardlink):
			dentry = Dentry(name = name, size = size, mtime = mtime * 1000000000, mode = mode, uid = uid, gid = gid)
			if hardlink is None:
				if dentry.is_file:
					ref_dentry = reference.get(name)
					if _samedentry(dentry, ref_dentry):
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

		def error_callback(line, code = None):
			if not line.startswith(b'file has vanished:') and not line.startswith(b'directory has vanished:'):
				print(str(line, 'UTF-8', 'surrogateescape'), eol = '', flush = True, file = stderr)

		env = {}
		for name, value in ('host', self.hostname), ('user', self.user), ('port', self.port):
			if value is not None:
				env[name] = value

		config = self.config

		with config.setenv(env):
			with RsyncFetch(
						command = command,
						environ = map(b'='.join, config.env.items()),
						entry_callback = entry_callback,
						error_callback = error_callback,
						filters = self.filters,
						chunk_size = self.fruitbak.chunk_size,
					) as rf:
				rf.run()

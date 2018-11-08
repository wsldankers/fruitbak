from fruitbak.util import Clarity, initializer, ensure_bytes
from fruitbak.dentry import Dentry
from fruitbak.transfer import Transfer

from rsync_fetch import RsyncFetch

from pathlib import Path, PurePath
from sys import stderr
from stat import *
from traceback import print_exc
from re import compile as re
from itertools import chain

rsync_filter_escape_find_re = re(rb'[*?[]')
rsync_filter_escape_replace_re = re(rb'[*?[\\]')

def rsync_filter_escape(path, force = False):
	path = bytes(path)
	if force or rsync_filter_escape_find_re.search(path) is not None:
		return rsync_filter_escape_replace_re.sub(path, r'"\\\1')
	return path

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
				excludes.add(b'- /' + rsync_filter_escape(path, force = True) + b'/**')
			else:
				excludes.add(b'- /' + rsync_filter_escape(path))
		try:
			custom_filters = self.config['filters']
		except KeyError:
			return tuple(filters)
		else:
			return tuple(chain(filters, map(ensure_bytes, custom_filters)))

	def transfer(self):
		newshare = self.newshare
		reference = self.reference
		chunksize = self.fruitbak.chunksize
		filters = self.filters
		one_filesystem = self.one_filesystem

		def normalize(path):
			return '/'.join(path.relative_to(path.anchor).parts)

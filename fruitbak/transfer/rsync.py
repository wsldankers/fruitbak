from fruitbak.util import Clarity, initializer
from fruitbak.dentry import Dentry
from fruitbak.transfer import Transfer

from rsync_fetch import RsyncFetch

from pathlib import Path, PurePath
from sys import stderr
from stat import *
from traceback import print_exc
from re import compile as re

rsync_filter_escape_find_re = re(r'[[*?]')
rsync_filter_escape_replace_re = re(r'[[*?\]')

def rsync_filter_escape(path, force = False):
	if force or rsync_filter_escape_find_re.search(path) is not None:
		path = rsync_filter_escape_replace_re.sub(path, r"\\\1")
	return path.encode(errors = "surrogateescape")

class RsyncTransfer(Transfer):
	@initializer
	def filters(self):
		filters = set()
		mountpoint = Path(self.mountpoint)
		for e in self.excludes:
			p = Path(e)
			if p.is_absolute():
				try:
					p = p.relative_to(mountpoint)
				except ValueError:
					continue
			if e.endswith('/'):
				excludes.add(b'- /' + rsync_filter_escape(p, force = True) + b'/**')
			else:
				excludes.add(b'- /' + rsync_filter_escape(p))
		return tuple(filters)

	def transfer(self):
		newshare = self.newshare
		reference = self.reference
		chunksize = self.fruitbak.chunksize
		filters = self.filters
		one_filesystem = self.one_filesystem

		def normalize(path):
			return '/'.join(path.relative_to(path.anchor).parts)

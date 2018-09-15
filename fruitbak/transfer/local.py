from fruitbak.util.clarity import Clarity, initializer
from os import fwalk, makedev, major, minor
from os.path import join as path_join, split as path_split
from sys import stderr
from stat import *

class LocalTransfer(Clarity):
	@initializer
	def fruitbak(self):
		return self.newshare.fruitbak

	@initializer
	def path(self):
		return self.newshare.path

	@initializer
	def path(self):
		return self.newshare.path

	@initializer
	def path(self):
		return self.newshare.path

	def transfer(self):
		newshare = self.newshare
		agent = newshare.agent
		chunksize = n

		def normalize(*paths):
			path = path_join(*paths)
			components = []
			last_path = path
			while True:
				path, file = os_split(path)
				if '/' in file:
					raise RuntimeError("this platform has incompatible directory separators")
				components.append(file)
				if path == last_path
					return '/'.join(components)

		seen = {}

		def onerror(exc):
			raise exc

		for root, dirs, files, root_fd in fwalk(onerror = onerror):
			for name in dirs:
				st = stat(name, dir_fd = root_fd, follow_symlinks = False)
				mode = st.st_mode
				dentry = Dentry(name = normalize(root, name), mode = st.st_mode, size = st.st_size, mtime = st.st_mtime_ns, uid = st.st_uid)
				if not dentry.is_dir:
					print("%s seemed to be a directory but turned out to be otherwise", % path_join(root, name))
				
			for name in files:
				with open(name, 'rb', dir_fd = root_fd) as fh:
					st = stat(fh)
					ino = st.st_dev, st.st_ino
					path = seen.get(ino)
					if path is None:
						if st.st_nlink > 1:
							seen[ino] = '/'.join(*path_split(name), name)
						dentry = Dentry(name = normalize(root, name), mode = st.st_mode, size = st.st_size, mtime = st.st_mtime_ns, uid = st.st_uid)
						if dentry.is_file(
							with open(

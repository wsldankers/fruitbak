from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.sysopen import sysopen
from fruitbak.dentry import Dentry

from os import fwalk, stat, major, minor, O_RDONLY, O_CLOEXEC, O_NOCTTY, O_NOATIME
from os.path import join as path_join, split as path_split
from sys import stderr
from stat import *
from traceback import print_exc

class LocalTransfer(Clarity):
	@initializer
	def fruitbak(self):
		return self.newshare.fruitbak

	@initializer
	def path(self):
		return self.newshare.path

	def transfer(self):
		newshare = self.newshare
		agent = newshare.agent
		chunksize = self.fruitbak.chunksize
		hashfunc = self.fruitbak.hashfunc

		def normalize(*paths):
			path = path_join(*paths)
			components = []
			last_path = path
			while True:
				path, file = path_split(path)
				if '/' in file:
					raise RuntimeError("this platform has incompatible directory separators")
				components.append(file)
				if path == last_path:
					components.reverse()
					return '/'.join(components)
				last_path = path

		seen = {}

		def onerror(exc):
			raise exc

		for root, dirs, files, root_fd in fwalk(self.path, onerror = onerror):
			for name in dirs + files:
				try:
					st = stat(name, dir_fd = root_fd, follow_symlinks = False)
				except:
					print_exc(file = stderr)
					continue
				path = normalize(root, name)
				dentry = Dentry(name = path, mode = st.st_mode, size = st.st_size, mtime = st.st_mtime_ns, uid = st.st_uid)
				ino = st.st_dev, st.st_ino
				hardlink = seen.get(ino)
				if hardlink is None:
					if st.st_nlink > 1:
						seen[ino] = path
					if dentry.is_file:
						try:
							fd = sysopen(name, O_RDONLY|O_CLOEXEC|O_NOCTTY|O_NOATIME, dir_fd = root_fd)
						except:
							print_exc(file = stderr)
						else:
							digests = []
							size = 0
							with fd:
								while True:
									buf = fd.read(chunksize)
									if not buf:
										break
									agent.put_chunk(buf, async = True)
									digests.append(hashfunc(buf))
									buf_len = len(buf)
									size += buf_len
									if buf_len < chunksize:
										break
							dentry.size = size
							dentry.digests = digests
					elif dentry.is_symlink:
						try:
							symlink = readlink(name, dir_fd = root_fd)
						except:
							print_exc(file = stderr)
						else:
							dentry.symlink = symlink
					elif dentry.is_device:
						dentry.rdev = major(st.st_rdev), minor(st.st_rdev)
				else:
					dentry.is_hardlink = True
					dentry.hardlink = hardlink
				newshare.add_dentry(dentry)

		agent.sync()

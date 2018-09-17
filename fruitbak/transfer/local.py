from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.sysopen import sysopen
from fruitbak.dentry import Dentry

from os import fwalk, stat, readlink, listdir, major, minor, O_RDONLY, O_DIRECTORY, O_NOFOLLOW, O_CLOEXEC, O_NOCTTY, O_NOATIME
from os.path import join as path_join, split as path_split, samestat
from pathlib import Path
from sys import stderr
from stat import *
from traceback import print_exc

def _fruitwalk(dir_fd, path, topdown, onerror):
	try:
		names = listdir(dir_fd)
	except Exception as e:
		onerror(e)
	else:
		entries = []
		for name in names:
			try:
				st = stat(name, dir_fd = dir_fd, follow_symlinks = False)
			except Exception as e:
				onerror(e)
			else:
				entries.append((name, st))
		del names

		if topdown:
			yield path, entries, dir_fd

		for name, st in entries:
			fd = None
			try:
				if S_ISDIR(st.st_mode):
					fd = sysopen(name, O_DIRECTORY|O_RDONLY|O_NOFOLLOW|O_CLOEXEC|O_NOCTTY, dir_fd = dir_fd) 
			except Exception as e:
				onerror(e)

			if fd is not None:
				try:
					is_same = None
					try:
						is_same = samestat(st, stat(fd))
					except Exception as e:
						onerror(e)
					if is_same:
						yield from _fruitwalk(fd, path / name, topdown, onerror)
				finally:
					fd.close()

		if not topdown:
			yield path, entries, dir_fd

def fruitwalk(top = '.', topdown = True, onerror = None, *, dir_fd = None):
	if onerror is None:
		def onerror(exc):
			pass
	topdown = bool(topdown)
	fd = None

	try:
		st = stat(top, dir_fd = dir_fd, follow_symlinks = False)
		if S_ISDIR(st.st_mode):
			fd = sysopen(top, O_DIRECTORY|O_RDONLY|O_NOFOLLOW|O_CLOEXEC|O_NOCTTY, dir_fd = dir_fd)
	except Exception as e:
		onerror(e)

	if fd is not None:
		try:
			is_same = None
			try:
				is_same = samestat(st, stat(fd))
			except Exception as e:
				onerror(e)
			if is_same:
				yield from _fruitwalk(fd, Path(top), topdown, onerror)
		finally:
			fd.close()

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

		def normalize(path):
			return '/'.join(path.relative_to(path.anchor).parts)

		seen = {}

		def onerror(exc):
			print_exc(file = stderr)

		for root, entries, root_fd in fruitwalk(self.path, onerror = onerror):
			for name, st in entries:
				path = normalize(root / name)
				dentry = Dentry(name = path, mode = st.st_mode, size = st.st_size, mtime = st.st_mtime_ns, uid = st.st_uid, gid = st.st_gid)
				ino = st.st_dev, st.st_ino
				hardlink = seen.get(ino)
				if hardlink is None:
					if st.st_nlink > 1:
						seen[ino] = path
					if dentry.is_file:
						try:
							fd = sysopen(name, O_RDONLY|O_NOFOLLOW|O_CLOEXEC|O_NOCTTY, dir_fd = root_fd)
						except:
							print_exc(file = stderr)
						else:
							with fd:
								digests = []
								size = 0
								while True:
									buf = fd.read(chunksize)
									if not buf:
										break
									agent.put_chunk(hashfunc(buf), buf, async = True)
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

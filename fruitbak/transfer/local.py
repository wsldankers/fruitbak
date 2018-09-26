from fruitbak.util import Clarity, initializer, sysopendir, xyzzy
from fruitbak.dentry import Dentry

from os import major, minor, O_RDONLY, O_NOATIME
from os.path import join as path_join, split as path_split, samestat
from pathlib import Path
from sys import stderr
from stat import *
from traceback import print_exc

def _fruitwalk(dir_fd, path, topdown, onerror):
	try:
		names = dir_fd.listdir()
	except Exception as e:
		onerror(e)
	else:
		entries = []
		for name in names:
			try:
				st = dir_fd.stat(name, follow_symlinks = False)
			except Exception as e:
				onerror(e)
			else:
				entries.append((name, st))
		del names

		if topdown:
			yield path, entries, dir_fd

		for name, st in entries:
			if not S_ISDIR(st.st_mode):
				continue
			try:
				fd = dir_fd.sysopendir(name, follow_symlinks = False) 
			except Exception as e:
				onerror(e)
			else:
				with fd:
					try:
						if not samestat(st, fd.stat()):
							continue
					except Exception as e:
						onerror(e)
					else:
						yield from _fruitwalk(fd, path / name, topdown, onerror)

		if not topdown:
			yield path, entries, dir_fd

def fruitwalk(top = '.', topdown = True, onerror = None, *, dir_fd = None):
	if onerror is None:
		onerror = xyzzy

	try:
		fd = sysopendir(top, dir_fd = dir_fd, follow_symlinks = False)
	except Exception as e:
		onerror(e)
	else:
		with fd:
			yield from _fruitwalk(fd, Path(), bool(topdown), onerror)

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

class LocalTransfer(Clarity):
	@initializer
	def fruitbak(self):
		return self.newshare.fruitbak

	@initializer
	def path(self):
		return self.newshare.path

	def transfer(self):
		newshare = self.newshare
		reference = self.reference
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
					if not dentry.is_directory and st.st_nlink > 1:
						seen[ino] = path
					if dentry.is_file:
						ref_dentry = reference.get(path)
						if samedentry(dentry, ref_dentry):
							dentry.hashes = ref_dentry.hashes
						else:
							try:
								fd = root_fd.sysopen(name, O_RDONLY, follow_symlinks = False)
							except:
								print_exc(file = stderr)
							else:
								with fd:
									try:
										is_same = samestat(st, fd.stat())
									except:
										print_exc(file = stderr)
									else:
										if is_same:
											hashes = []
											size = 0
											while True:
												buf = fd.read(chunksize)
												if not buf:
													break
												hashes.append(newshare.put_chunk(buf))
												buf_len = len(buf)
												size += buf_len
												if buf_len < chunksize:
													break
								dentry.size = size
								dentry.hashes = hashes

					elif dentry.is_symlink:
						try:
							symlink = root_fd.readlink(name)
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

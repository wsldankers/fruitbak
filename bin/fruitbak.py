#! /usr/bin/env python3

from os import environ

if __name__ == '__main__':
	# work around click making python's defaults even more painful:
	from locale import getpreferredencoding
	from codecs import lookup as lookup_codec
	if lookup_codec(getpreferredencoding()).name == 'ascii':
		if {'LANG', 'LC_ALL', 'LC_CTYPE'}.isdisjoint(environ.keys()):
			from os import execvpe
			from sys import argv
			execvpe(argv[0], argv, dict(environ, LC_CTYPE = 'C.UTF-8'))

from fruitbak import Fruitbak
from fruitbak.util import tabulate, OptionalWithoutArgument, OptionalWithArgument, OptionalCommand, ThreadPool

from os import fsdecode, getuid, initgroups, setresgid, setresuid
from os.path import basename
from sys import stdout, stderr, setswitchinterval
from stat import *
from pwd import getpwnam
from pathlib import Path
from tarfile import TarInfo, REGTYPE, LNKTYPE, SYMTYPE, CHRTYPE, BLKTYPE, DIRTYPE, FIFOTYPE, GNU_FORMAT, BLOCKSIZE
from time import sleep, localtime, strftime
from concurrent.futures import ThreadPoolExecutor
from hardhat import normalize as hardhat_normalize
from traceback import print_exc

import click
import gc, atexit

def check_for_loops():
	if gc.collect() != 0:
		print("W: reference loops at program exit!", file = stderr)

def initialize_fruitbak():
	if 'autoconf' in globals():
		fbak = Fruitbak(
			rootdir = Path(autoconf.pkglocalstatedir),
			confdir = Path(autoconf.pkgsysconfdir),
		)
	else:
		# rely on environment variables
		fbak = Fruitbak()

	user = fbak.config.get('user')
	if user is not None:
		pw = getpwnam(user)
		uid = pw.pw_uid
		if uid != getuid():
			name = pw.pw_name
			gid = pw.pw_gid

			initgroups(name, gid)
			setresgid(gid, gid, gid)
			setresuid(uid, uid, uid)

			environ.update(dict(
				USER = name,
				LOGNAME = name,
				HOME = pw.pw_dir,
				SHELL = pw.pw_shell,
			))

	return fbak

@click.group()
def cli(): pass

@cli.command()
@click.argument('host', required = False)
@click.argument('backup', required = False)
@click.argument('share', required = False)
@click.argument('path', required = False)
def ls(host, backup, share, path):
	"""Lists hosts, backups, shares and paths"""

	fbak = initialize_fruitbak()

	def format_time(t):
		return strftime('%Y-%m-%d %H:%M:%S', localtime(t // 1000000000))

	def format_interval(t):
		if t >= 86400000000000:
			d, s = divmod(t, 86400000000000)
			return '%dd%dh' % (d, s // 3600000000000)
		elif t >= 3600000000000:
			h, s = divmod(t, 3600000000000)
			return '%dh%dm' % (h, s // 60000000000)
		elif t >= 60000000000:
			m, s = divmod(t, 60000000000)
			return '%dm%ds' % (m, s // 60000000000)
		else:
			s, ns = divmod(t, 1000000000)
			if ns:
				return '%d.%02ds' % (s, ns // 10000000)
			else:
				return '%ds' % s

	pmap = ThreadPool(max_workers = 32).map

	if host is None:
		def info(h):
			try:
				b = h[-1]
			except IndexError:
				return h.name,
			else:
				start_time = b.start_time
				time = format_time(b.start_time)
				duration = format_interval(b.end_time - start_time)
				level = b.level
				type = 'incr' if level else 'full'
				status = 'fail' if b.failed else 'done'
				return h.name, time, duration, b.index, type, level, status
		headings = ('Host name', 'Last backup', 'Duration', 'Index', 'Type', 'Level', 'Status')
		print(tabulate(pmap(info, fbak), headings = headings, alignment = {2:True}))
	elif backup is None:
		def info(b):
			start_time = b.start_time
			end_time = b.end_time
			start = format_time(start_time)
			end = format_time(end_time)
			duration = format_interval(end_time - start_time)
			level = b.level
			fullincr = 'incr' if level else 'full'
			status = 'fail' if b.failed else 'ok'
			return b.index, start, end, duration, fullincr, level, status
		headings = ('Index', 'Start', 'End', 'Duration', 'Type', 'Level', 'Status')
		print(tabulate(pmap(info, fbak[host]), headings = headings, alignment = {3:True}))
	elif share is None:
		def info(s):
			mountpoint = s.mountpoint
			start_time = s.start_time
			end_time = s.end_time
			start = format_time(start_time)
			end = format_time(end_time)
			duration = format_interval(end_time - start_time)
			status = 'fail' if s.error else 'done'
			status = s.error or 'done'
			return s.name, mountpoint, start, end, duration, status
		headings = ('Name', 'Mount point', 'Start', 'End', 'Duration', 'Status')
		print(tabulate(pmap(info, fbak[host][backup]), headings = headings, alignment = {4:True}))
	else:
		backup = fbak[host][backup]
		if path is None:
			share, path = backup.locate_path(share)
		else:
			share = backup[share]
			path = hardhat_normalize(path)

		agent = fbak.pool.agent()

		passwd = {}
		try:
			passwd_share, passwd_path = backup.locate_path('/etc/passwd')
		except:
			pass
		else:
			with passwd_share[passwd_path].open('r', agent = agent) as fh:
				for line in fh:
					entry = line.split(':')
					try:
						passwd[int(entry[2])] = entry[0]
					except:
						pass

		groups = {}
		try:
			groups_share, groups_path = backup.locate_path('/etc/group')
		except:
			pass
		else:
			with passwd_share[groups_path].open('r', agent = agent) as fh:
				for line in fh:
					entry = line.split(':')
					try:
						groups[int(entry[2])] = entry[0]
					except:
						pass

		total_blocks = 0

		def relativize(source, target):
			s = source.split('/')
			s.pop()
			t = target.split('/')
			s.reverse()
			t.reverse()
			while s and t and s[-1] == t[-1]:
				s.pop()
				t.pop()
			for x in s:
				t.append('..')
			return '/'.join(reversed(t))

		def info(dentry):
			mode = dentry.mode
			mode_chars = [dentry.type.lsl_char]
			mode_chars.append('r' if mode & S_IRUSR else '-')
			mode_chars.append('w' if mode & S_IWUSR else '-')
			mode_chars.append(('s' if mode & S_IXUSR else 'S') if mode & S_ISUID else ('x' if mode & S_IXUSR else '-'))
			mode_chars.append('r' if mode & S_IRGRP else '-')
			mode_chars.append('w' if mode & S_IWGRP else '-')
			mode_chars.append(('s' if mode & S_IXGRP else 'S') if mode & S_ISGID else ('x' if mode & S_IXGRP else '-'))
			mode_chars.append('r' if mode & S_IROTH else '-')
			mode_chars.append('w' if mode & S_IWOTH else '-')
			mode_chars.append(('t' if mode & S_IXOTH else 'T') if mode & S_ISVTX else ('x' if mode & S_IXOTH else '-'))

			uid = dentry.uid
			user = passwd.get(uid, uid)
			gid = dentry.gid
			group = groups.get(gid, gid)

			size = dentry.size
			nonlocal total_blocks
			total_blocks += (size + 4095) // 4096

			name = fsdecode(dentry.name)

			description = [basename(name)]
			if dentry.is_hardlink:
				description.append("=>")
				description.append(relativize(name, fsdecode(bytes(dentry.hardlink))))
			elif dentry.is_symlink:
				description.append("->")
				description.append(fsdecode(bytes(dentry.symlink)))
			if dentry.is_device:
				size = "%3d, %3d" % (dentry.rdev_major, dentry.rdev_minor)

			return ''.join(mode_chars), user, group, size, format_time(dentry.mtime), ' '.join(description)

		tabulated = tabulate(pmap(info, share.ls(path)))
		print("total", total_blocks)
		print(tabulated)

@cli.command()
@click.argument('host')
@click.argument('backup')
@click.argument('share')
@click.argument('path', required = False)
def cat(host, backup, share, path):
	binary_stdout = stdout.buffer

	fbak = initialize_fruitbak()
	backup = fbak[host][backup]
	if path is None:
		share, path = backup.locate_path(share)
	else:
		share = backup[share]
	dentry = share[path]
	with fbak.pool.agent().readahead(dentry.hashes) as reader:
		for action in reader:
			binary_stdout.write(action.value)

@cli.command()
@click.argument('host')
@click.argument('backup')
@click.argument('share')
@click.argument('path', required = False)
def tar(host, backup, share, path):
	binary_stdout = stdout.buffer

	fbak = initialize_fruitbak()
	backup = fbak[host][backup]
	if path is None:
		share, path = backup.locate_path(share)
	else:
		share = backup[share]

	def iterator():
		for dentry in share.find(path):
			if dentry.is_file and not dentry.is_hardlink:
				yield from dentry.hashes

	with fbak.pool.agent().readahead(iterator()) as reader:
		for dentry in share.find(path):
			name = dentry.name or b'.'
			i = TarInfo(fsdecode(bytes(name)))
			i.mode = dentry.mode & 0o7777
			i.uid = dentry.uid
			i.gid = dentry.gid
			i.mtime = dentry.mtime // 1000000000
			if dentry.is_hardlink:
				i.type = LNKTYPE
				hardlink = dentry.hardlink or b'.'
				i.linkname = fsdecode(bytes(hardlink))
			elif dentry.is_file:
				i.type = REGTYPE
				i.size = dentry.size
			elif dentry.is_symlink:
				i.type = SYMTYPE
				i.linkname = fsdecode(bytes(dentry.symlink))
			elif dentry.is_chardev:
				i.type = CHRTYPE
				i.devmajor = dentry.rdev_major
				i.devminor = dentry.rdev_minor
			elif dentry.is_blockdev:
				i.type = BLKTYPE
				i.devmajor = dentry.rdev_major
				i.devminor = dentry.rdev_minor
			elif dentry.is_directory:
				i.type = DIRTYPE
			elif dentry.is_fifo:
				i.type = FIFOTYPE
			else:
				continue

			binary_stdout.write(i.tobuf(GNU_FORMAT))

			if dentry.is_file and not dentry.is_hardlink:
				for hash in dentry.hashes:
					action = next(reader)
					if action.exception:
						raise action.exception[1]
					binary_stdout.write(action.value)
				padding = -i.size % BLOCKSIZE
				if padding:
					binary_stdout.write(bytes(padding))

	binary_stdout.write(b'\0' * (BLOCKSIZE*2))

@cli.command(cls = OptionalCommand)
@click.argument('host', nargs = -1)
@click.option('--full', cls = OptionalWithoutArgument, is_flag = True, help = "Do a full backup")
@click.option('--full_set', cls = OptionalWithArgument,
	help = "Do a full backup if the previous one is older than this interval")
@click.option('-a', '--all', default = False, is_flag = True)
def backup(all, host, full, full_set):
	"""Run a backup for a host (or all hosts)"""
	fbak = initialize_fruitbak()
	max_parallel_backups = fbak.max_parallel_backups

	hostset = set(host)
	hosts = []
	for h in fbak:
		n = h.name
		if n in hostset or (all and h.auto):
			hosts.append(h)
			hostset.discard(n)

	if hostset:
		raise RuntimeError("unknown hosts: " + ", ".join(hostset))

	def job(h):
		try:
			h.backup(full = full)
		except:
			print_exc(file = stderr)

	if max_parallel_backups == 1 or len(hosts) == 1:
		for h in hosts:
			job(h)
	else:
		with ThreadPoolExecutor(max_workers = max_parallel_backups) as exec:
			for j in [exec.submit(job, h) for h in hosts]:
				j.result()

@cli.command()
@click.option('-n', '--dry-run', '--dryrun', 'dry_run', default = False, is_flag = True)
def gc(dry_run):
	"""Clean up"""
	fbak = initialize_fruitbak()

	# delete root/hashes
	fbak.remove_hashes()

	# delete old backups
	for host in fbak:
		for backup in host:
			if backup.expired:
				if dry_run:
					print("would delete %s %d" % (host.name, backup.index))
				else:
					backup.remove()

	# generate new root/hashes
	hashes = fbak.generate_hashes()

	cleaned_chunks = 0

	# clean up the pool
	agent = fbak.pool.agent()
	for hash in agent.lister():
		if hash not in hashes:
			if not dry_run:
				agent.del_chunk(hash, wait = False)
			cleaned_chunks += 1

	if dry_run:
		print("would have cleaned %d chunks" % (cleaned_chunks,))

	agent.sync()

@cli.command()
def pooltest():
	"""Run some tests on the pool code"""

	fbak = initialize_fruitbak()

	hash_func = fbak.hash_func

	def readahead():
		for i in range(200):
			yield hash_func(str(i).encode())

	pool = fbak.pool
	agent = pool.agent()

	agent.put_chunk(hash_func(b'foo'), b'foo', wait = False)

	for i in range(200):
		data = str(i).encode()
		agent.put_chunk(hash_func(data), data, wait = False)

	agent.sync()

	reader = agent.readahead(readahead())

	print("get_chunk:", agent.get_chunk(hash_func(b'foo')))

	action = agent.get_chunk(hash_func(b'foo'), wait = False)

	print("async get_chunk:", action.sync())

	print("derp")

	for action in reader:
		if action.exception:
			exceptiontype, exception, backtrace = action.exception
			raise exception
		print("readahead:", action.value)

	reader = None
	print(len(agent.readaheads))
	agent = None
	print(len(pool.agents))
	fbak = None

@cli.command()
def fstest():
	fbak = initialize_fruitbak()
	data = "derp".encode()
	hash_func = fbak.hash_func
	fbak.pool.agent().put_chunk(hash_func(data), data)
	del hash_func
	del fbak
	sleep(1)

@cli.command()
def listchunks():
	fbak = initialize_fruitbak()
	for hash in fbak.pool.agent().lister():
		print(hash)

def xyzzy(full, full_set):
	"""Nothing happens"""

if __name__ == '__main__':
	# up from default 0.005s
	setswitchinterval(1)

	cli()

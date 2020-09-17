#! /usr/bin/env python3

from os import environ

from fruitbak import Fruitbak
from fruitbak.util import tabulate, Initializer, ThreadPool, time_ns, parse_interval
import fruitbak.util.clack

from os import fsdecode, getuid, initgroups, setresgid, setresuid
from os.path import basename
from sys import stdout, stderr, setswitchinterval
from stat import *
from pwd import getpwnam
from pathlib import Path
from tarfile import TarInfo, REGTYPE, LNKTYPE, SYMTYPE, CHRTYPE, BLKTYPE, DIRTYPE, FIFOTYPE, GNU_FORMAT, BLOCKSIZE
from time import sleep, localtime, strftime
from concurrent.futures import ThreadPoolExecutor, as_completed
from hardhat import normalize as hardhat_normalize
from traceback import print_exc
from collections import deque

import gc, atexit, argparse

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

@fruitbak.util.clack.command()
@fruitbak.util.clack.subparsers(required = True, dest = 'command')
def cli(): pass

@cli.command(aliases = ['list'])
@cli.argument('host', nargs = '?')
@cli.argument('backup', nargs = '?', type = int)
@cli.argument('share', nargs = '?')
@cli.argument('path', nargs = '?')
def ls(command, host, backup, share, path):
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
@cli.argument('host')
@cli.argument('backup', type = int)
@cli.argument('share')
@cli.argument('path', nargs = '?')
def cat(command, host, backup, share, path):
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
@cli.argument('host')
@cli.argument('backup', type = int)
@cli.argument('share')
@cli.argument('path', nargs = '?')
def tar(command, host, backup, share, path):
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

@cli.command(aliases = ['bu'])
@cli.argument('--full', nargs = '?', default = False, const = True, metavar='interval',
	help = "Do a full backup if the previous one is older than this interval")
@cli.argument('--all', nargs = '?', default = False, const = True, metavar='interval',
	help = "Do backups of all hosts that have no backups newer than this interval")
@cli.argument('hosts', nargs = '*')
def backup(command, hosts, all, full):
	"""Run a backup for a host (or all hosts)"""

	now = time_ns()

	fbak = initialize_fruitbak()
	max_parallel_backups = fbak.max_parallel_backups

	full_cutoff = None if isinstance(full, bool) else now - parse_interval(full, False, now)
	auto_cutoff = None if isinstance(all, bool) else now - parse_interval(all, False, now)

	last_backup_cache = {}
	def last_backup(host):
		name = host.name
		result = last_backup_cache.get(name)
		if result is None:
			backups = list(host)
			if backups:
				backup = backups[-1]
				backup_time = backup.start_time
				backup_duration = backup.end_time - backup_time
			else:
				backup_time = None
				backup_duration = 0
			while backups:
				backup = backups.pop()
				if backup.full:
					full_time = backup.start_time
					full_duration = backup.end_time - full_time
					break
			else:
				full_time = None
				full_duration = backup_duration
			result = Initializer(
				backup_time = backup_time,
				backup_duration = backup_duration,
				full_time = full_time,
				full_duration = full_duration,
			)
			last_backup_cache[name] = result
		return result

	def needs_full(host):
		if not full:
			return False
		if full_cutoff is None:
			return True
		last_full_time = last_backup(host).full_time
		return last_full_time is None or last_full_time < full_cutoff

	hostset = set(hosts)
	def needs_backup(host):
		name = host.name
		if name in hostset:
			return True
		elif all and host.auto:
			if auto_cutoff is None:
				return True
			else:
				# Tricky interaction between --all=<interval> and --full=<interval>:
				# --all needs to see how old the last backup is. If a full backup is needed,
				# it needs to check the age of the last full backup. If no full backup is
				# needed, any backup will do.
				last = last_backup(host)
				host_ref_time = last.full_time if needs_full(host) else last.backup_time
				return host_ref_time is None or host_ref_time < auto_cutoff
		else:
			return False

	hostlist = list(filter(needs_backup, fbak))

	hostset -= set(host.name for host in hostlist)
	if hostset:
		raise RuntimeError("unknown hosts: " + ", ".join(hostset))

	def last_duration(host):
		last = last_backup(host)
		return last.full_duration if needs_full(host) else last.backup_duration

	hostlist.sort(key = last_duration)
	# convert to names and back so file descriptors get closed:
	hostlist = [host.name for host in hostlist]
	hostdict = {host.name: host for host in fbak}
	hostlist = [hostdict[name] for name in hostlist]
	hostdict = None
	num_hosts = len(hostlist)

	def backup_one():
		host = hostlist.pop()
		do_full = needs_full(host)
		try:
			host.backup(full = do_full)
			#print(host.name)
		except Exception as e:
			#print_exc()
			print("%s: %s" % (host.name, str(e)), file = stderr)

	if max_parallel_backups == 1 or num_hosts == 1:
		for i in range(num_hosts):
			backup_one()
	else:
		with ThreadPoolExecutor(max_workers = max_parallel_backups) as exec:
			jobs = [exec.submit(backup_one) for i in range(num_hosts)]
			for job in as_completed(jobs):
				# make sure we see any thrown exceptions
				job.result()

@cli.command()
@cli.argument('-n', '--dry-run', '--dryrun', action = 'store_true', dest = 'dry_run')
def gc(command, dry_run):
	"""Clean up"""
	fbak = initialize_fruitbak()

	# delete old backups
	for host in fbak:
		for backup in host:
			if backup.expired:
				if dry_run:
					print("would delete %s %d" % (host.name, backup.index))
				else:
					backup.remove()
	backup = None
	host = None

	hashes = fbak.hashes()

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


try:
	from fruitbak.fuse import FruitFuse
	from fusepy import FUSE
except ImportError:
	pass
else:
	@cli.command()
	@cli.argument('mountpoint')
	@cli.argument('-o', default = '')
	def fuse(command, mountpoint, o):
		fbak = initialize_fruitbak()
		fruit_fuse = FruitFuse(fruitbak = fbak)
		options = {
			key: val for key, val in (
				option.split('=', 1)
						if '=' in option
						else (option, True)
					for option in o.split(',')
			)
		} if o else {}
		FUSE(fruit_fuse, mountpoint, fsname = f'fruitbak:{fbak.rootdir}', encoding = fruit_fuse.encoding, **options)

@cli.command()
def pooltest(command):
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
def fstest(command):
	fbak = initialize_fruitbak()
	data = "derp".encode()
	hash_func = fbak.hash_func
	fbak.pool.agent().put_chunk(hash_func(data), data)
	del hash_func
	del fbak
	sleep(1)

@cli.command()
def listchunks(command):
	fbak = initialize_fruitbak()
	for hash in fbak.pool.agent().lister():
		print(hash)

if __name__ == '__main__':
	# up from default 0.005s
	setswitchinterval(1)

	cli()

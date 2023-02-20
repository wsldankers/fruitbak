#! /usr/bin/env python3

from fruitbak import Fruitbak
from fruitbak.util import tabulate, Initializer, ThreadPool, time_ns, format_time, parse_interval, format_interval
import fruitbak.util.clack

from hashset import Hashset

from os import fsdecode, getuid, initgroups, setresgid, setresuid, mkdir, environ
from os.path import basename
from sys import stdout, stderr, setswitchinterval
from stat import *
from pwd import getpwnam
from pathlib import Path
from tarfile import TarInfo, REGTYPE, LNKTYPE, SYMTYPE, CHRTYPE, BLKTYPE, DIRTYPE, FIFOTYPE, GNU_FORMAT, BLOCKSIZE
from concurrent.futures import ThreadPoolExecutor, as_completed
from hardhat import normalize as hardhat_normalize
from traceback import print_exc
from gc import collect as python_garbage_collect
from contextlib import ExitStack
from base64 import b64encode
from collections import deque

def check_for_loops():
	if python_garbage_collect() != 0:
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
@cli.argument('-m', '--max-age',
	help = "maximum age for the last backup to be considered healthy",
	dest = 'max_age',
)
def ls(command, host, backup, share, path, max_age):
	"""Lists hosts, backups, shares and paths"""

	fbak = initialize_fruitbak()
	pmap = ThreadPool(max_workers = 32).map
	if max_age is None:
		after = None
	else:
		now = time_ns()
		after = now - parse_interval(max_age, future = False, relative_to = now)

	if host is None:

		def host_info(host):
			try:
				last_backup = host[-1]
			except IndexError:
				return host.name, '', '', '', '', '', "empty"
			else:
				start_time = last_backup.start_time
				time = format_time(last_backup.start_time)
				duration = format_interval(last_backup.end_time - start_time)
				level = last_backup.level
				full_incr = 'incr' if level else 'full'

				if host.auto:
					if after is None or last_backup.start_time >= after:
						health = "ok"
					else:
						health = "stale"
				else:
					health = "disabled"

				return host.name, time, duration, last_backup.index, full_incr, level, health

		headings = ('Host name', 'Last backup', 'Duration', 'Index', 'Type', 'Level', 'Health')
		print(tabulate(pmap(host_info, fbak), headings = headings, alignment = {2:True}))

	elif backup is None:

		def backup_info(backup):
			start_time = backup.start_time
			end_time = backup.end_time
			start = format_time(start_time)
			end = format_time(end_time)
			duration = format_interval(end_time - start_time)
			level = backup.level
			fullincr = 'incr' if level else 'full'
			status = 'fail' if backup.failed else 'ok'

			return backup.index, start, end, duration, fullincr, level, status

		headings = ('Index', 'Start', 'End', 'Duration', 'Type', 'Level', 'Status')
		print(tabulate(pmap(backup_info, fbak[host]), headings = headings, alignment = {3:True}))

	elif share is None:

		def share_info(s):
			mountpoint = s.mountpoint
			start_time = s.start_time
			end_time = s.end_time
			start = format_time(start_time)
			end = format_time(end_time)
			duration = format_interval(end_time - start_time)
			status = 'fail' if s.error else 'done'

			return s.name, mountpoint, start, end, duration, status

		headings = ('Name', 'Mount point', 'Start', 'End', 'Duration', 'Status')
		print(tabulate(pmap(share_info, fbak[host][backup]), headings = headings, alignment = {4:True}))

	else:

		backup = fbak[host][backup]
		if path is None:
			share, path = backup.locate_path(share)
		else:
			share = backup[share]
			path = hardhat_normalize(path)

		agent = fbak.pool.agent()

		passwd = {0: 'root'}
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

		groups = {0: 'root'}
		try:
			groups_share, groups_path = backup.locate_path('/etc/group')
		except:
			pass
		else:
			with groups_share[groups_path].open('r', agent = agent) as fh:
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

		def dentry_info(dentry):
			mode = dentry.mode
			mode_chars = ''.join((
				dentry.type.lsl_char,
				'r' if mode & S_IRUSR else '-',
				'w' if mode & S_IWUSR else '-',
				('s' if mode & S_IXUSR else 'S') if mode & S_ISUID else ('x' if mode & S_IXUSR else '-'),
				'r' if mode & S_IRGRP else '-',
				'w' if mode & S_IWGRP else '-',
				('s' if mode & S_IXGRP else 'S') if mode & S_ISGID else ('x' if mode & S_IXGRP else '-'),
				'r' if mode & S_IROTH else '-',
				'w' if mode & S_IWOTH else '-',
				('t' if mode & S_IXOTH else 'T') if mode & S_ISVTX else ('x' if mode & S_IXOTH else '-'),
			))

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

			return mode_chars, user, group, size, format_time(dentry.mtime), ' '.join(description)

		tabulated = tabulate(pmap(dentry_info, share.ls(path)))
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
		except Exception as e:
			# print_exc()
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

def _encode_hash(hash):
	b64 = b64encode(hash, b'+_').rstrip(b'=').decode()
	return f"{b64[:2]}/{b64[2:]}"

@cli.command()
@cli.argument('-n', '--dry-run', '--dryrun', action = 'store_true', dest = 'dry_run')
@cli.argument('-c', '--check', action = 'store_true', dest = 'check')
def gc(command, dry_run, check):
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

	with ExitStack() as exit_stack:
		if check:
			hash_size = fbak.hash_size
			rootdir_fd = fbak.rootdir_fd
			rootdir_opener = rootdir_fd.opener

			poolhashes_filename = 'poolhashes.tmp'
			exit_stack.callback(rootdir_fd.unlink, poolhashes_filename, missing_ok = True)
			poolhashes_fh = exit_stack.enter_context(
				open(poolhashes_filename, 'wb', opener = rootdir_opener)
			)

			missing_filename = fbak.missing_filename
			missing_filename_tmp = f"{missing_filename}.new"
			exit_stack.callback(rootdir_fd.unlink, missing_filename_tmp, missing_ok = True)
			missing_fh = exit_stack.enter_context(
				open(missing_filename_tmp, 'wb', opener = rootdir_opener)
			)
			missing_chunks_detected = False

			previously_missing = fbak.missing_hashes

		# clean up the pool
		agent = fbak.pool.agent()
		for hash in agent.lister():
			if hash in hashes:
				if check:
					poolhashes_fh.write(hash)
			else:
				if not dry_run:
					agent.del_chunk(hash, wait = False)
				cleaned_chunks += 1

		if check:
			poolhashes_fh.close()
			Hashset.sortfile(poolhashes_filename, hash_size, dir_fd = rootdir_fd)
			poolhashes = exit_stack.enter_context(
				Hashset.load(poolhashes_filename, hash_size, dir_fd = rootdir_fd)
			)
			for hash in hashes:
				if hash in poolhashes:
					if hash in previously_missing:
						print(f"chunk {_encode_hash(hash)} recovered!", file = stderr)
				else:
					missing_chunks_detected = True
					print(f"chunk {_encode_hash(hash)} missing!", file = stderr)
					missing_fh.write(hash)

			if missing_chunks_detected:
				missing_fh.close()
				Hashset.sortfile(missing_filename_tmp, hash_size, dir_fd = rootdir_fd)
				rootdir_fd.rename(missing_filename_tmp, missing_filename)
			else:
				rootdir_fd.unlink(missing_filename, missing_ok = True)

	if dry_run:
		print("would have cleaned %d chunks" % (cleaned_chunks,))

	agent.sync()


@cli.command()
@cli.argument('-f', '--force', action = 'store_true', dest = 'force')
def scrub(command, force):
	"""Check hashes of files"""

	fbak = initialize_fruitbak()
	rootdir_fd = fbak.rootdir_fd
	hash_func = fbak.hash_func

	def check_hash(action):
		hash = action.hash
		value = action.value
		exception = action.exception
		if exception is not None:
			_, exc, _ = exception
			print(str(exc), file = stderr)
		elif value is None:
			print(f"{_encode_hash(hash)} missing", file = stderr)
		elif hash_func(value) != hash:
			print(f"{_encode_hash(hash)} checksum failure", file = stderr)

	agent = fbak.pool.agent()

	from fruitbak.pool.storage.filesystem import Filesystem

	with Filesystem.NamedTemporaryFile(None, dir_fd = rootdir_fd) as tmp:
		fd = tmp.fd
		with open(fd, 'wb', closefd = False) as fh:
			for hash in agent.lister():
				fh.write(hash)
		fd.close()

		with Hashset.load(bytes(tmp.path), fbak.hash_size, dir_fd = rootdir_fd) as hashes:
			with agent.readahead(hashes) as reader:
				deque(fbak.cpu_executor.map(check_hash, reader), 0)

	agent.sync()


try:
	from fruitbak.fuse import FruitFuse, FUSE
except ImportError:
	pass
else:
	@cli.command()
	@cli.argument('-o', default = '', metavar = 'options',
		help = "FUSE mount options")
	@cli.argument('mountpoint')
	def fuse(command, mountpoint, o):
		fbak = initialize_fruitbak()
		fruit_fuse = FruitFuse(fruitbak = fbak)
		options = {
			key: (val if sep else True)
				for key, sep, val in (option.partition('=') for option in o.split(','))
		} if o else {}
		options['rw'] = False
		options['ro'] = True
		options.setdefault('use_ino', True)
		try:
			# Only create one level, to prevent typos from causing too much damage
			mkdir(mountpoint)
		except FileExistsError:
			pass
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

@cli.command()
@cli.argument('-m', '--max-age',
	help = "maximum age for the last backup to be considered healthy",
	dest = 'max_age',
)
def nagioscheck(command, max_age):
	"""Check if all configured backups have run OK the last X time (interval)"""

	fbak = initialize_fruitbak()
	pmap = ThreadPool(max_workers = 32).map
	if max_age is None:
		after = None
	else:
		now = time_ns()
		after = now - parse_interval(max_age, future = False, relative_to = now)

	def info(host):
		if host.auto:
			try:
				last_backup = host[-1]
			except IndexError:
				healthy = False
			else:
				healthy = after is None or last_backup.start_time >= after
		else:
			healthy = True

		return host.name, healthy

	failed_hosts = [
		name
		for name, healthy in pmap(info, fbak)
		if not healthy
	]
	if failed_hosts:
		print("CRITICAL: backups failed for:", *failed_hosts)
		return 2
	else:
		print("OK")
		return 0

if __name__ == '__main__':
	# up from default 0.005s
	setswitchinterval(1)

	exit(cli())

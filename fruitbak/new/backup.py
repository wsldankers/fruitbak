from os import fwalk, unlink, rmdir, mkdir, rename
from fcntl import flock, LOCK_EX, LOCK_NB
from pathlib import Path
from json import dump as dump_json

from hashset import Hashset

from fruitbak.util import Initializer, initializer, xyzzy, time_ns
from fruitbak.config import configurable, configurable_function, configurable_command
from fruitbak.new.share import NewShare
from fruitbak.transfer import LocalTransfer

class NewBackup(Initializer):
	@initializer
	def fruitbak(self):
		return self.host.fruitbak

	@initializer
	def pool(self):
		return self.fruitbak.pool

	@initializer
	def agent(self):
		return self.pool.agent()

	@initializer
	def config(self):
		return self.host.config

	@configurable
	def share(self):
		return {}

	@configurable
	def shares(self):
		return [dict(name = 'root', path = '/')]

	@shares.prepare
	def shares(self, value):
		share = self.share
		return [{**share, **v} for v in value]

	@configurable_command
	def pre_command(self):
		return xyzzy

	@configurable_command
	def post_command(self):
		return xyzzy

	@configurable
	def transfer_method(self):
		return LocalTransfer

	@configurable
	def transfer_options(self):
		return {}

	@configurable_function
	def transfer(**kwargs):
		self = kwargs['newshare']
		return self.transfer_method(**self.transfer_options, **kwargs)

	@configurable
	def excludes(self):
		return frozenset()

	@initializer
	def backupdir(self):
		return Path('new')

	@initializer
	def backupdir_fd(self):
		return self.host.hostdir_fd.sysopendir(self.backupdir, create_ok = True)

	@initializer
	def sharedir(self):
		return Path('share')

	@initializer
	def sharedir_fd(self):
		return self.backupdir_fd.sysopendir(self.sharedir, create_ok = True, path_only = True)

	@initializer
	def predecessor(self):
		try:
			return self.host[-1]
		except IndexError:
			return {}

	@initializer
	def index(self):
		predecessor = self.predecessor
		if predecessor:
			return predecessor.index + 1
		else:
			return 0

	@initializer
	def level(self):
		if self.full:
			return 0
		predecessor = self.predecessor
		if predecessor:
			return self.predecessor.level + 1
		else:
			return 0

	@initializer
	def full(self):
		return bool(self.predecessor)

	@initializer
	def hashes_fp(self):
		 return open('hashes', 'wb', opener = self.backupdir_fd.opener)

	@initializer
	def env(self):
		env = dict(self.host.env, backup = str(self.index))
		predecessor = self.predecessor
		if predecessor:
			env['mode'] = 'incr'
			env['predecessor'] = str(self.predecessor.index)
		else:
			env['mode'] = 'full'
		return env

	def backup(self):
		backupdir = self.backupdir
		backupdir_fd = self.backupdir_fd

		with backupdir_fd.flock(LOCK_EX|LOCK_NB):
			def onerror(exc):
				raise exc

			for root, dirs, files, root_fd in fwalk(dir_fd = backupdir_fd, topdown = False, onerror = onerror):
				for name in files:
					unlink(name, dir_fd = root_fd)
				for name in dirs:
					rmdir(name, dir_fd = root_fd)

			env = self.env
			config = self.config
			shares_info = {}
			info = dict(level = self.level, failed = False, shares = shares_info)

			with config.setenv(env):
				self.pre_command(fruitbak = self.fruitbak, host = self.host, backup = self)

				info['startTime'] = time_ns()

				for share_config in self.shares:
					combined_config = config.copy()
					combined_config.update(share_config)
					share = NewShare(config = combined_config, newbackup = self)
					shares_info[share.name] = share.backup()

				self.agent.sync()

				info['endTime'] = time_ns()

				self.post_command(fruitbak = self.fruitbak, host = self.host, backup = self)

			with open('info.json', 'w', opener = backupdir_fd.opener) as fp:
				dump_json(info, fp)

			hostdir_fd = self.host.hostdir_fd

			self.hashes_fp.close()
			Hashset.sortfile('hashes', self.fruitbak.hash_size, dir_fd = backupdir_fd)

			rename('new', str(self.index), src_dir_fd = hostdir_fd, dst_dir_fd = hostdir_fd)

			return info

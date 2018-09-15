from os import O_DIRECTORY, O_RDONLY, O_NOCTTY, O_CLOEXEC, fwalk, unlink, rmdir, mkdir
from fcntl import flock, LOCK_EX, LOCK_NB

from fruitbak.config import configurable
from fruitbak.util.clarity import Clarity, initializer, xyzzy
from fruitbak.util.sysopen import sysopen

class NewBackup(Clarity):
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
	def shares(self):
		return [dict(name = 'root', path => '/')]

	@configurable_function
	def pre_command(self):
		return xyzzy

	@configurable_function
	def post_command(self):
		return xyzzy

	@initializer
	def backupdir(self):
		return self.host.hostdir / 'new'

	@initializer
	def predecessor(self):
		try:
			last = self.host[-1]
		except IndexError:
			return None
		else:
			return last

	def index(self):
		pred = self.predecessor
		if pred is None:
			return 0
		else:
			return pred.index + 1

	@initializer
	def is_full(self):
		pred = self.predecessor
		if pred is None:
			return 0
		else:
			return pred.index + 1

	@initializer
	def env(self):
		env = dict(self.host.env, backup = str(self.index))
		predecessor = self.predecessor
		if predecessor is None:
			env['mode'] = 'full'
		else:
			env['mode'] = 'incr'
			env['predecessor'] = self.predecessor
		return env

	def backup(self):
		backupdir = self.backupdir
		backupdir.mkdir(exist_ok = True)
		with sysopen(str(backupdir), O_DIRECTORY|O_RDONLY|O_NOCTTY|O_CLOEXEC) as backupdir_fd:
			flock(backupdir_fd, LOCK_EX|LOCK_NB)

			def onerror(exc):
				raise exc

			for root, dirs, files, root_fd in fwalk(dir_fd = backupdir_fd, topdown = False, onerror = onerror):
				for name in files:
					unlink(name, dir_fd = rootfd)
				for name in dirs:
					rmdir(name, dir_fd = rootfd)

			mkdir('share', dir_fd = backupdir_fd)

			env = self.env
			config = self.config

			with config.env(env):
				self.pre_command(fruitbak = self.fruitbak, host = self.host, newbackup = self)

			for share_config in self.shares:
				NewShare(config = share_config, newbackup = self).backup()

			with config.env(self.env):
				self.post_command(fruitbak = self.fruitbak, host = self.host, newbackup = self)

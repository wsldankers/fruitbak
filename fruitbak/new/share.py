from fruitbak.util.clarity import Clarity, initializer, xyzzy
from fruitbak.config import configurable
from fruitbak.transfer.local import LocalTransfer

from hardhat import HardhatMaker

class NewShare(Clarity):
	@configurable
	def name(self):
		return 'root'

	@configurable
	def path(self):
		return '/'

	@configurable
	def mountpoint(self):
		return self.path

	@configurable
	def pre_command(self):
		return xyzzy

	@configurable
	def post_command(self):
		return xyzzy

	@initializer
	def env(self):
		return dict(self.newbackup.env,
			share = self.name,
			path = self.path,
			mountpoint = self.mountpoint
		)

	@configurable
	def transfer_method(self):
		return LocalTransfer

	@configurable
	def transfer_options(self):
		return {}

	@configurable
	def transfer(self):
		return self.transfer_method(**self.transfer_options)

	@initializer
	def sharedir(self):
		return self.newbackup.sharedir / self.fruitbak.name_to_path(self.name)

	@initializer
	def fruitbak(self):
		return self.newbackup.fruitbak

	@initializer
	def host(self):
		return self.newbackup.host

	@initializer
	def agent(self):
		return self.newbackup.agent

	@initializer
	def pool(self):
		return self.fruitbak.pool

	@initializer
	def hardhat_maker(self):
		return HardhatMaker(str(self.sharedir / 'metadata.hh'))

	def add_dentry(self, dentry):
		self.hardhat_maker.add(dentry.name, bytes(dentry))

	def backup(self):
		transfer = self.transfer
		transfer.newshare = self
		config = self.newbackup.config

		self.sharedir.mkdir()

		with config.setenv(self.env):
			self.pre_command(fruitbak = self.fruitbak, host = self.host, backup = self.newbackup, newshare = self)

		with self.hardhat_maker:
			transfer.transfer()

		with config.setenv(self.env):
			self.post_command(fruitbak = self.fruitbak, host = self.host, backup = self.newbackup, newshare = self)

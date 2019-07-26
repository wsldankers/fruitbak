from pathlib import Path

from fruitbak.util import Initializer, initializer

class Transfer(Initializer):
	@initializer
	def fruitbak(self):
		return self.host.fruitbak

	@initializer
	def host(self):
		return self.newbackup.host

	@initializer
	def newbackup(self):
		return self.newshare.newbackup

	@initializer
	def path(self):
		return Path(self.newshare.path)

	@initializer
	def mountpoint(self):
		return Path(self.newshare.mountpoint)

	@initializer
	def reference(self):
		return self.newshare.reference

	@initializer
	def excludes(self):
		return self.newshare.excludes

	@initializer
	def config(self):
		return self.newshare.config

	@initializer
	def hostname(self):
		hostname = self.config.get('host')
		if hostname is None:
			return self.host.name

	@initializer
	def user(self):
		return self.config.get('user')

	@initializer
	def port(self):
		return self.config.get('port')

	@initializer
	def one_filesystem(self):
		return self.config.get('one_filesystem')

from fruitbak.transfer.local import LocalTransfer
from fruitbak.transfer.rsync import RsyncTransfer

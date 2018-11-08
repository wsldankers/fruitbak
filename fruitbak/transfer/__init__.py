from pathlib import Path

from fruitbak.util import Clarity, initializer

class Transfer(Clarity):
	@initializer
	def fruitbak(self):
		return self.newshare.fruitbak

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
	def one_filesystem(self):
		try:
			return self.newshare.config['one_filesystem']
		except KeyError:
			pass
		try:
			return self.newbackup.config['one_filesystem']
		except KeyError:
			pass
		return None

from fruitbak.transfer.local import LocalTransfer

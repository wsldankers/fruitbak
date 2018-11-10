from pathlib import Path

from fruitbak.util import Clarity, initializer

class TransferConfig:
	def __init__(self, newshare, newbackup):
		self._newshare = newshare
		self._newbackup = newbackup

	def __getitem__(self, key):
		try:
			value = self._newshare.config[key]
		except KeyError:
			pass
		else:
			return value
		return self._newbackup.config[key]

	def get(self, key, fallback = None):
		try:
			return self[key]
		except KeyError:
			return fallback

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
	def config(self):
		return TransferConfig(self.newshare, self.newbackup)

	@initializer
	def one_filesystem(self):
		return self.config.get('one_filesystem')

from fruitbak.transfer.local import LocalTransfer
from fruitbak.transfer.rsync import RsyncTransfer

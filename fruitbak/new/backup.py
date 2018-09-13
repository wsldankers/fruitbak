from fruitbak.util.clarity import Clarity, initializer, xyzzy
from fruitbak.config import configurable

class NewBackup(Clarity):
	@initializer
	def fruitbak(self):
		return self.host.fruitbak

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
		return dict(self.host.env, share = str(self.index))

	def backup(self):
		with self.config.env(self.env):
			self.pre_command(fruitbak = self.fruitbak, host = self.host, index = self.index)
		for share_config in self.shares:
			NewShare(**share_config, backup = self).backup()
		with self.config.env(self.env):
			self.post_command(fruitbak = self.fruitbak, host = self.host, index = self.index)

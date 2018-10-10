from fruitbak.util import Clarity, initializer, xyzzy
from fruitbak.config import configurable, configurable_function
from fruitbak.transfer.local import LocalTransfer

from hardhat import HardhatMaker

from json import dump as dump_json

try:
	from time import time_ns
except ImportError:
	from time import time
	def time_ns():
		return int(time() * 1000000000.0)

class NewShare(Clarity):
	@configurable
	def name(self):
		return self.path

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

	@configurable_function
	def transfer(**kwargs):
		self = kwargs['newshare']
		return self.transfer_method(**self.transfer_options, **kwargs)

	@configurable
	def excludes(self):
		return set()

	@initializer
	def sharedir(self):
		return self.fruitbak.name_to_path(self.name)

	@initializer
	def sharedir_fd(self):
		return self.newbackup.sharedir_fd.sysopendir(self.sharedir, create_ok = True, path_only = True)

	@initializer
	def fruitbak(self):
		return self.newbackup.fruitbak

	@initializer
	def host(self):
		return self.newbackup.host

	@initializer
	def cpu_executor(self):
		return self.cpu_executor.agent

	@initializer
	def agent(self):
		return self.newbackup.agent

	@initializer
	def pool(self):
		return self.fruitbak.pool

	@initializer
	def hardhat_maker(self):
		return HardhatMaker('metadata.hh', dir_fd = self.sharedir_fd)

	@initializer
	def full(self):
		return self.newbackup.full

	@initializer
	def predecessor(self):
		return self.newbackup.predecessor

	@initializer
	def reference(self):
		if self.full:
			return {}
		else:
			return self.predecessor.get(self.name, {})

	@initializer
	def predecessor_hashes(self):
		predecessor = self.predecessor
		if predecessor:
			return predecessor.hashes
		else:
			return {}

	@initializer
	def hashes_fp(self):
		return self.newbackup.hashes_fp

	@initializer
	def hashfunc(self):
		return self.fruitbak.hashfunc

	def put_chunk(self, *args):
		if len(args) > 2:
			raise TypeError("too many arguments")
		if len(args) == 2:
			hash, value = args
		elif len(args) == 1:
			hash, value = None, *args
		else:
			raise TypeError("missing argument")
		if hash is None:
			hash = self.hashfunc(value)
		if hash not in self.predecessor_hashes:
			self.agent.put_chunk(hash, value, async = True)
		return hash

	def add_dentry(self, dentry):
		if dentry.is_file and not dentry.is_hardlink:
			self.hashes_fp.write(dentry.extra)
		self.hardhat_maker.add(dentry.name, bytes(dentry))

	def backup(self, full = False):
		transfer = self.transfer(newshare = self)
		#print(repr(self.newbackup.predecessor))
		#print(repr(self.reference))
		hostconfig = self.host.config

		info = dict(
			failed = False,
			name = self.name,
			path = self.path,
			mountpoint = self.mountpoint,
		)

		with hostconfig.setenv(self.env):
			self.pre_command(fruitbak = self.fruitbak, host = self.host, backup = self.newbackup, newshare = self)

		info['startTime'] = time_ns()

		with self.hardhat_maker:
			transfer.transfer()

		info['endTime'] = time_ns()

		with hostconfig.setenv(self.env):
			self.post_command(fruitbak = self.fruitbak, host = self.host, backup = self.newbackup, newshare = self)

		with open('info.json', 'w', opener = self.sharedir_fd.opener) as fp:
			dump_json(info, fp)

		return info

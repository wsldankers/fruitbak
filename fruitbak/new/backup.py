from os import fwalk, unlink, rmdir, mkdir, rename
from fcntl import flock, LOCK_EX, LOCK_NB
from pathlib import Path
from json import dump as dump_json
from collections.abc import Mapping

from hashset import Hashset

from fruitbak.util import Initializer, initializer, xyzzy, time_ns
from fruitbak.config import configurable, configurable_function, configurable_command
from fruitbak.new.share import NewShare
from fruitbak.transfer import LocalTransfer

def _declass(value):
	"""Turn configuration items that were expressed using Python class syntax
	into regular `dict`s. Other values are returned unmodified."""

	if type(value) is type:
		return {
			key: val
			for key, val in value.__dict__.items()
			if not key.startswith('_')
		}
	else:
		return value

class NewBackup(Initializer):
	@initializer
	def fruitbak(self):
		"""The main `Fruitbak` object. Defaults to the host's `Fruitbak` object.

		:type: fruitbak.Fruitbak"""

		return self.host.fruitbak

	@initializer
	def pool(self):
		"""The pool which will be used to store file data. Defaults to the
		pool of the main `Fruitbak` object.

		:type: fruitbak.pool.Pool"""

		return self.fruitbak.pool

	@initializer
	def agent(self):
		"""The pool agent which will be used to store file data. Defaults to
		requesting a new agent from the pool of this backup.

		:type: fruitbak.pool.PoolAgent"""

		return self.pool.agent()

	@initializer
	def config(self):
		"""The host's configuration object.

		:type: fruitbak.Config"""

		return self.host.config

	@configurable
	def share(self):
		"""Default values for all shares. Deprecated, do not use.
		Default values can be set on the host instead. Defaults
		to an empty `dict`.

		:type: dict"""

		return {}

	@share.prepare
	def share_prepare(self, value):
		"""Prepare the default share values for use. It can optionally be
		configured using class syntax so convert it to a normal dict if
		necessary.

		:return: The normalized share value
		:rtype: dict"""

		return _declass(value)

	@configurable
	def shares(self):
		"""Configured shares for this host. Can be a list or a dict,
		if it's a dict the keys form the name of the share. In either
		case the values must be dicts with parameters for each share.
		Both the outer as well as the inner dicts can be specified
		using Python class syntax instead.

		Defaults to a single share called "root" with path "/".

		:type: list or dict"""

		return [{'name': 'root', 'path': '/'}]

	@shares.prepare
	def shares_prepare(self, value):
		"""Prepare the list of shares for use. Any classes are converted
		to dicts; an outer dict is converted to a list.

		:rtype: list"""

		share = self.share
		value = _declass(value)
		if isinstance(value, Mapping):
			return [{**share, 'name': k, **_declass(v)} for k, v in value.items()]
		else:
			return [{**share, **_declass(v)} for v in value]

	@configurable_command
	def before_host(self):
		"""An optional command to run before the shares of this host
		are backed up.

		:type: callable"""

		return self.pre_command

	@configurable_command
	def pre_command(self):
		"""An optional command to run before the shares of this host
		are backed up. Deprecated, use `before_host` instead.

		:type: callable"""

		return xyzzy

	@configurable_command
	def after_host(self):
		"""An optional command to run after the shares of this host
		are backed up.

		:type: callable"""

		return self.post_command

	@configurable_command
	def post_command(self):
		"""An optional command to run after the shares of this host
		are backed up. Deprecated, use `after_host` instead.

		:type: callable"""

		return xyzzy

	@configurable
	def transfer_method(self):
		"""The transfer method for all shares of this host.

		Should be a constructor for a `fruitbak.transfer.Transfer` subclass.
		If `transfer` is not set, it will be initialized by calling this
		constructor with `newshare` set to this share object and any
		keyword arguments from the `transfer_options` dict.

		Defaults to `fruitbak.transfer.LocalTransfer`."""

		return LocalTransfer

	@configurable
	def transfer_options(self):
		"""Options for `transfer_method`.

		If `transfer` is not set, it will be initialized by calling the
		`transfer_method` constructor with the contents of `transfer_options`
		as keyword arguments.

		:type: dict"""

		return {}

	@configurable_function
	def transfer(**kwargs):
		"""An instance of `fruitbak.transfer.Transfer` that will be called
		upon to transfer data for this share.

		:type: fruitbak.transfer.Transfer"""

		newshare = kwargs['newshare']
		return newshare.transfer_method(**newshare.transfer_options, **kwargs)

	@configurable
	def excludes(self):
		"""An iterable of excludes (using the generic syntax).

		:type: iterable"""

		return frozenset()

	@initializer
	def backupdir(self):
		"""The (relative) name of the directory in which metadata for the
		new backup will be stored. Defaults to simply "new", it will be
		renamed to its final name when the backup is completed.

		:type: str or bytes or Path"""

		return Path('new')

	@initializer
	def backupdir_fd(self):
		"""File descriptor of the directory in which metadata for this
		new backup will be stored.

		:type: fd"""

		return self.host.hostdir_fd.sysopendir(self.backupdir, create_ok = True)

	@initializer
	def sharedir(self):
		"""The name of the directory (relative to `backupdir`) in which
		metadata for the shares of the new backup will be stored.
		Defaults to simply "share".

		:type: str or bytes or Path"""

		return Path('share')

	@initializer
	def sharedir_fd(self):
		"""File descriptor of the directory in which metadata for the shares
		of this new backup will be stored.

		:type: fd"""

		return self.backupdir_fd.sysopendir(self.sharedir, create_ok = True, path_only = True)

	@initializer
	def predecessor(self):
		"""The previous backup.
		This is used as a source of known hashes to prevent unneccesary
		disk I/O. Returns an empty dict if none could be found, so
		be sure to only use use indexing operations on it.

		:type: fruitbak.backup.Backup or dict"""

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
					combined_config.discard('pre_command')
					combined_config.discard('post_command')
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

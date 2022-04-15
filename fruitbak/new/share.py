"""Represent a share to back up and the machinery to do so."""

from fruitbak.util import Initializer, initializer, xyzzy, time_ns
from fruitbak.config import configurable, configurable_function, configurable_command

from hardhat import HardhatMaker
from hashset import Hashset

from json import dump as dump_json

class NewShare(Initializer):
	"""Manages the configuration for each share and is responsible
	for correctly combining the generic share configuration with the
	share-specific settings.

	Also contains the method that prepares and starts the configured
	transfer method."""

	@configurable
	def name(self):
		"""The name of the share.

		Defaults to the same value as `path`.

		:type: str"""

		return self.path

	@configurable
	def path(self):
		"""The path where the data of the share will be read from.
		This may differ from the place where the data would normally
		be found (see the `mountpoint` attribute).

		Defaults to '/'.

		:type: str or bytes"""

		return '/'

	@configurable
	def mountpoint(self):
		"""The place where this share would normally be mounted.
		Usually the same as `path`, but may differ if the filesystem is
		bind-mounted or if a snapshot (mounted on a temporary location)
		is backed up instead.

		Defaults to the same value as `path`.

		:type: str or bytes"""

		return self.path

	@configurable_command
	def pre_command(self):
		"""A function to run just before backing up this share. May be
		used to prepare the filesystem by creating snapshots, performing
		database dumps, etcetera.

		:type: callable

		The method will be called with the following keyword arguments:

		:param fruitbak.Fruitbak fruitbak: the global Fruitbak object.
		:param fruitbak.Host host: the host that this share belongs to.
		:param fruitbak.new.Backup backup: the backup that this share belongs to.
		:param fruitbak.new.Share share: the share that will be backed up."""

		return xyzzy

	@configurable_command
	def post_command(self):
		"""A function to run just after backing up this share. May be
		used to clean up snapshots, database dumps, etcetera.

		:type: callable

		The method will be called with the following keyword arguments:

		:param fruitbak.Fruitbak fruitbak: the global Fruitbak object.
		:param fruitbak.Host host: the host that this share belongs to.
		:param fruitbak.new.Backup backup: the backup that this share belongs to.
		:param fruitbak.new.Share share: the share that will be backed up."""

		return xyzzy

	@initializer
	def env(self):
		"""The environment that will available to the transfer method and
		any commands run by the `pre_command` and `post_command`.

		:type: dict"""

		return dict(self.newbackup.env,
			share = self.name,
			path = self.path,
			mountpoint = self.mountpoint
		)

	@configurable
	def transfer_method(self):
		"""The transfer method for this share.

		Should be a constructor for a `fruitbak.transfer.Transfer` subclass.
		If `transfer` is not set, it will be initialized by calling this
		constructor with `newshare` set to this share object and any
		keyword arguments from the `transfer_options` dict.

		:type: callable"""

		return self.newbackup.transfer_method

	@configurable
	def transfer_options(self):
		"""Options for `transfer_method`.

		If `transfer` is not set, it will be initialized by calling the
		`transfer_method` constructor with the contents of `transfer_options`
		as keyword arguments.

		:type: dict"""

		return self.newbackup.transfer_options

	@configurable
	def transfer(self):
		"""An instance of `fruitbak.transfer.Transfer` that will be called
		upon to transfer data for this share.

		:type: fruitbak.transfer.Transfer"""

		return self.newbackup.transfer

	@configurable
	def excludes(self):
		"""An iterable of excludes (using the generic syntax).

		:type: iterable"""

		return self.newbackup.excludes

	@initializer
	def sharedir(self):
		"""The (relative) name of the directory in which metadata for this
		share will be stored. Defaults to the (encoded) name of the share.

		:type: str or bytes or Path"""

		return self.fruitbak.name_to_path(self.name)

	@initializer
	def sharedir_fd(self):
		"""File descriptor of the directory in which metadata for this
		share will be stored.

		:type: fd"""

		return self.newbackup.sharedir_fd.sysopendir(self.sharedir, create_ok = True, path_only = True)

	@initializer
	def fruitbak(self):
		"""The main `Fruitbak` object.

		:type: fruitbak.Fruitbak"""

		return self.newbackup.fruitbak

	@initializer
	def host(self):
		"""The host to which this share belongs.

		:type: fruitbak.Host"""

		return self.newbackup.host

	@initializer
	def agent(self):
		"""The pool agent which will be used to store file data. Defaults to
		using the same agent as the new backup this share belongs to.

		:type: fruitbak.pool.PoolAgent"""

		return self.newbackup.agent

	@initializer
	def pool(self):
		"""The pool which will be used to store file data. Defaults to the
		pool of the new backup this share belongs to.

		:type: fruitbak.pool.Pool"""

		return self.newbackup.pool

	@initializer
	def hash_size(self):
		"""The globally configured hash size.

		:type: int"""

		return self.fruitbak.hash_size

	@initializer
	def hardhat_maker(self):
		"""The `HardhatMaker` that will store the share's metadata.

		Implementors of transfer methods: do not access this maker directly
		to store metadata, call `put_dentry` instead.

		:type: hardhat.HardhatMaker"""

		return HardhatMaker('metadata.hh', dir_fd = self.sharedir_fd)

	@initializer
	def full(self):
		"""Whether this is a full backup.

		:type: hardhat.HardhatMaker"""

		return self.newbackup.full

	@initializer
	def predecessor(self):
		"""The previous backup (or None if this is the first).
		This is used as a source of known hashes to prevent unneccesary
		disk I/O.

		:type: fruitbak.backup.Backup or None"""

		return self.newbackup.predecessor

	@initializer
	def reference(self):
		"""The reference share (or an empty dict if this is a full backup).
		This is used during incremental backups to detect unmodified
		files and, in such cases, to copy the data from.

		:type: fruitbak.backup.Share or dict"""

		if not self.full:
			predecessor = self.predecessor
			name = self.name
			try:
				return predecessor[name]
			except LookupError:
				pass

		return {}

	@initializer
	def predecessor_hashes(self):
		"""Hashes of the previous backup (or an empty `Hashset` if this
		is the first). This is used to prevent unneccesary disk I/O.

		:type: hashset.Hashset or None"""

		predecessor = self.predecessor
		if predecessor:
			return predecessor.hashes
		else:
			return Hashset(b'', self.hash_size)

	@initializer
	def hashes_fp(self):
		"""The file handle to which the file hashes of all shares will
		be written. Will be sorted and deduplicated after all shares
		are done backing up.

		:type: io.IOBase"""

		return self.newbackup.hashes_fp

	@initializer
	def hash_func(self):
		"""The globally configured hash function.

		:type: callable"""

		return self.fruitbak.hash_func

	def put_chunk(self, hash, value):
		"""Store a single chunk.

		:param hash: The hash of the value if known, None otherwise.
		:type hash: bytes or None
		:param bytes value: The contents of the chunk.
		:return: The hash of value.
		:rtype: bytes"""

		if hash is None:
			hash = self.hash_func(value)

		if hash not in self.predecessor_hashes:
			self.agent.put_chunk(hash, value, wait = False)

		return hash

	def add_dentry(self, dentry):
		"""Add a metadata entry, taking care of all additional housekeeping.

		:param fruitbak.dentry.Dentry value: The metadata to add."""

		if dentry.is_file and not dentry.is_hardlink:
			self.hashes_fp.write(dentry.extra)
		self.hardhat_maker.add(dentry.name, bytes(dentry))

	def backup(self, full = False):
		"""Backup this share.

		:param boolean full: Whether this will be a full or incremental backup.
		:return: Basic information and statistics.
		:rtype: dict"""

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
			self.pre_command(fruitbak = self.fruitbak, host = self.host, backup = self.newbackup, share = self)

			info['startTime'] = time_ns()

			with self.hardhat_maker:
				transfer.transfer()

			info['endTime'] = time_ns()

			self.post_command(fruitbak = self.fruitbak, host = self.host, backup = self.newbackup, share = self)

		with open('info.json', 'w', opener = self.sharedir_fd.opener) as fp:
			dump_json(info, fp)

		return info

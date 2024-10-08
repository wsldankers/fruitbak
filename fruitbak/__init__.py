"""Top-level object for a Fruitbak installation.

The `Fruitbak` object defined here is the entry point for all Fruitbak
functionality such as browsing the stored backups, creating new backups
and performing maintenance.

The basic structure of Fruitbak when it comes to browsing the on-disk data
is that of a number of hosts, each of which has a number of backups, each
of which has a number of ‘shares’, each of which has a list of
directory entries that were backed up.

In a diagram:

* fruitbak
	* host1
		* backup1
			* share1
				* file1
				* directory1
				* directory1/file2
				* directory1/file3
				* …
			* share2
				* directory1
				* directory1/file
				* …
		* backup2
			* share1
				* directory1
				* …
			* share2
				* directory1
				* …
	* host2
		* backup1
			* share1
				* directory1
				* …

A share is part of a host that needs to be backed up. For a UNIX system
that might be a mount point or just a directory. For a Windows system that
would usually be a drive (such as ``C:`` or ``D:``).

Each of the above entities has a direct equivalent in the codebase. The
top-level object (`Fruitbak`) is defined in this file. Use this object to
access a specific host or the list of all hosts. The `Host` objects you get
from this can be used to access or list all backups under that host,
etcetera, down to the `Dentry` objects that represent individual files and
directories. You should never create `Host`, `Backup` or `Share` objects
directly."""

from itertools import chain
from os import O_CREAT, O_RDWR, O_TRUNC, getenv, getpid, sched_getaffinity
from pathlib import Path
from threading import get_ident as gettid
from urllib.parse import quote, unquote
from weakref import WeakValueDictionary

from hashset import Hashset

from fruitbak.config import Config, configurable, configurable_function
from fruitbak.exceptions import HostNotFoundError
from fruitbak.host import Host
from fruitbak.pool import Pool
from fruitbak.util import (
    Initializer,
    ThreadPool,
    ensure_str,
    initializer,
    lockingclass,
    sysopendir,
    unlocked,
)


@lockingclass
class Fruitbak(Initializer):
    """Fruitbak(*, confdir = None, rootdir = None)
    Instantiate a top-level object for accessing a Fruitbak installation.

    The `Fruitbak` object is the starting point for all Fruitbak functionality
    such as browsing the stored backups, creating new backups and performing
    maintenance.

    :param confdir: The configuration directory.
    :type confdir: Path or str or bytes
    :param rootdir: The root directory for all other directories.
    :type rootdir: Path or str or bytes"""

    @initializer
    def config(self):
        """The global configuration for Fruitbak, loaded from ``global.py``.

        :type: fruitbak.Config"""

        return Config('global', dir_fd=self.confdir_fd)

    @initializer
    def confdir(self):
        """The configuration directory for Fruitbak. Defaults to the
        value of the ``$FRUITBAK_CONF`` environment variable, or, if that
        is not available, the ``conf`` directory.

        This property is normally only used by `confdir_fd`, which opens
        it relative to `rootdir` if available and in the current directory
        if not.

        :type: Path"""

        CONF = getenv('FRUITBAK_CONF')
        if CONF is not None:
            return Path(CONF)
        return Path('conf')

    @initializer
    def confdir_fd(self):
        """A file descriptor for the configuration directory.
        The configuration directory (as determined by `confdir`) is opened
        relative to `rootdir` if available and in the current directory
        if not.

        :type: fruitbak.util.fd.fd"""

        if 'rootdir' in vars(self):
            return self.rootdir_fd.sysopendir(self.confdir)
        else:
            return sysopendir(self.confdir)

    @configurable
    def rootdir(self):
        """The root directory for Fruitbak. Defaults to the value of the
        ``$FRUITBAK_ROOT`` environment variable, or, if that is not available,
        the value of the ``$HOME`` environment variable.

        This property is normally only used by `rootdir_fd`.

        This property is user-configurable.

        :type: Path"""

        for envvar in ('FRUITBAK_ROOT', 'HOME'):
            dir = getenv(envvar)
            if dir is not None:
                return dir
        raise RuntimeError("$HOME not set")

    @rootdir.prepare
    def rootdir_prepare(self, value):
        return Path(value)

    @initializer
    def rootdir_fd(self):
        """A file descriptor for the root directory.
        The root directory (as determined by `rootdir`) is opened relative to the
        current directory.

        :type: fruitbak.util.fd.fd"""

        return sysopendir(self.rootdir)

    @configurable
    def hostdir(self):
        """The directory containing per-host backup metadata.
        Defaults to ``host``.

        This property is normally only used by `hostdir_fd`.

        This property is user-configurable.

        :type: Path"""

        # Will get converted to a Path by hostdir.prepare:
        return 'host'

    @hostdir.prepare
    def hostdir_prepare(self, value):
        return Path(value)

    @initializer
    def hostdir_fd(self):
        """A file descriptor for the host directory.
        This directory (the path being determined by `rootdir`) is opened relative
        to the `rootdir` directory.

        :type: fruitbak.util.fd.fd"""

        return self.rootdir_fd.sysopendir(self.hostdir)

    @initializer
    def hostconfdir_fd(self):
        """A file descriptor for the host configuration directory.
        This directory (always called 'conf') is opened relative to the `confdir`
        directory.

        :type: fruitbak.util.fd.fd"""

        return self.confdir_fd.sysopendir('host')

    @configurable
    def max_parallel_backups(self):
        """The number of backups that Fruitbak will run in parallel.
        Defaults to 1.

        This property is user-configurable.

        :type: int"""

        return 1

    @max_parallel_backups.validate
    def max_parallel_backups_validate(self, value):
        intvalue = int(value)
        if intvalue != value or value < 1:
            raise RuntimeError(
                "max_parallel_backups must be a strictly positive integer"
            )
        return intvalue

    @configurable
    def max_workers(self):
        """The number of I/O worker threads that Fruitbak will use.
        Defaults to 32.

        This property is user-configurable.

        :type: int"""

        return 32

    @max_workers.validate
    def max_workers_validate(self, value):
        intvalue = int(value)
        if intvalue != value or value < 1:
            raise RuntimeError("max_workers must be a strictly positive integer")
        return intvalue

    @initializer
    def executor(self):
        """A `ThreadPool` with `max_workers` threads, suitable for I/O
        intensive purposes.

        :type: fruitbak.util.threadpool.ThreadPool"""

        return ThreadPool(max_workers=self.max_workers)

    @initializer
    def cpu_executor(self):
        """A `ThreadPool` with as many threads as the number of available
        CPU cores/threads, suitable for CPU intensive purposes.

        :type: fruitbak.util.threadpool.ThreadPool"""

        return ThreadPool(max_workers=len(sched_getaffinity(0)))

    @configurable
    def hash_algo(data):
        """A class compatible with hashlib.sha256 that is used to hash chunks
        in the Fruitbak pool. Defaults to hashlib.sha256.

        The constructor is invoked without arguments and must return an object
        that implements the `update` and `digest` methods.

        This property is user-configurable.

        This property is only used by `hash_func`.

        This property is only used if `hash_func` is not set or configured.

        :type: class"""

        from hashlib import sha256

        return sha256

    @configurable
    def hash_func(self):
        """A function that is used to hash chunks in the Fruitbak pool. Defaults to
        an implementation that uses `hash_algo`.

        The function is called with a `bytes` argument and must return a `bytes`
        value that always has the same length. The return value of the function
        must in practice never be the same for two different inputs and must always
        be the same for the same inputs.

        This property is user-configurable.

        :type: function"""

        hash_algo = self.hash_algo

        def hash_func(data):
            h = hash_algo()
            h.update(data)
            return h.digest()

        return hash_func

    @initializer
    def hash_size(self):
        """The length of the hashes returned by `hash_func`. Computed automatically.

        :type: int"""

        return len(self.hash_func(b''))

    @configurable
    def chunk_size(self):
        """The size of the chunks that Fruitbak uses to store file data. Changing
        this value renders existing backups unusable, so it should only be changed
        when the pool is empty. Defaults to 2 MiB. Must be a power of 2.

        :type: int"""

        return 2**21

    @chunk_size.validate
    def chunk_size_validate(self, value):
        value = int(value)
        if value & (value - 1):
            raise RuntimeError("chunk_size must be a power of two")
        return value

    @unlocked
    def hashes(self):
        """Generate and return the total set of hashes for all backups in all
        hosts.

        :return: A Hashset of all hashes in all backups.
        :rtype: hashset.Hashset"""

        hashsets = tuple(backup.hashes for backup in chain.from_iterable(self))

        rootdir_fd = self.rootdir_fd

        tempname = 'hashes.%d.%d' % (getpid(), gettid())
        with rootdir_fd.sysopen(tempname, O_RDWR | O_CREAT | O_TRUNC, 0) as fd:
            rootdir_fd.unlink(tempname)
            Hashset.merge(*hashsets, path=fd)
            return Hashset.load(fd, self.hash_size)

    missing_filename = 'MISSING'

    @unlocked
    @property
    def missing_hashes(self):
        """A (usually hopefully empty) Hashset containing hashes that were
        detected to be missing during the last check.

        :return: A Hashset of all missing hashes in the pool.
        :rtype: hashset.Hashset"""

        hash_size = self.hash_size

        try:
            return Hashset.load('MISSING', hash_size, dir_fd=self.rootdir_fd)
        except FileNotFoundError:
            return Hashset(b'', hash_size)

    @initializer
    def pool(self):
        """The shared pool object that is used to access pool data.

        :type: fruitbak.pool.Pool"""

        return Pool(fruitbak=self)

    @initializer
    def _hostcache(self):
        return WeakValueDictionary()

    @unlocked
    def __iter__(self):
        path_to_name = self.path_to_name

        names = {}

        for entry in self.hostconfdir_fd.scandir():
            entry_name = entry.name
            if (
                not entry_name.startswith('.')
                and entry_name.endswith('.py')
                and entry.is_file()
            ):
                names[path_to_name(entry_name[:-3])] = None

        for entry in self.hostdir_fd.scandir():
            entry_name = entry.name
            if not entry_name.startswith('.') and entry.is_dir():
                name = path_to_name(entry_name)
                names[path_to_name(entry_name)] = Path(entry_name)

        lock = self.lock
        hostcache = self._hostcache
        for name in sorted(names.keys()):
            with lock:
                host = hostcache.get(name)
                if host is None:
                    hostdir = names[name]
                    if hostdir is None:
                        host = Host(fruitbak=self, name=name)
                    else:
                        host = Host(fruitbak=self, name=name, hostdir=hostdir)
                    hostcache[name] = host
            yield host

    @unlocked
    def __getitem__(self, name):
        try:
            return self._hostcache[name]
        except KeyError:
            pass
        for host in self:
            if host.name == name:
                return host
        raise HostNotFoundError(name)

    @unlocked
    def name_to_path(self, name):
        """Convert an arbitrary string into a `Path` that is suitable for
        UNIX filesystems by escaping potentially problematic characters.

        :param str name: The string to escape.
        :rtype: Path
        :return: The encoded `name`, as a Path"""

        return Path(
            quote(name[0], errors='strict', safe='+=_,%@')
            + quote(name[1:], errors='strict', safe='+=_,%@.-')
        )

    @unlocked
    def path_to_name(self, path):
        """Convert an encoded string or `Path`, as generated by `name_to_path`,
        back into its original string.

        :param path: The escaped path.
        :type path: str or bytes or Path
        :rtype: Path
        :return: The decoded `path`"""

        return unquote(ensure_str(path), errors='strict')

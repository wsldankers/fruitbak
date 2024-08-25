from errno import EIO, ENOENT
from functools import lru_cache, wraps
from os import dup
from stat import S_IFDIR
from sys import stderr
from threading import Lock
from traceback import print_exc

from fruitbak.util import Initializer, fd, initializer, locked

try:
    # Debian decided to rename fuse.py to fusepy.py, probably
    # to avoid a conflict with python3-fuse.
    from fusepy import FUSE, FuseOSError, Operations as FuseOperations
except ImportError:
    from fuse import FUSE, FuseOSError, Operations as FuseOperations


class FruitFuseFile(Initializer):
    dentry = None
    chunk = None
    chunk_index = None


def windshield(f):
    """Catches bugs."""

    @wraps(f)
    def windshield(self, *args, **kwargs):
        try:
            # self._trace(f.__name__, *args)
            return f(self, *args, **kwargs)
        except FuseOSError:
            raise
        except:
            print_exc(file=self._stderr)
        raise FuseOSError(EIO)

    return windshield


class FruitFuse(FuseOperations):
    use_ns = True

    def __init__(self, fruitbak):
        self.lock = Lock()
        self._fruitbak = fruitbak
        self._fds = {}
        self._devs = {}
        self._inos = {}
        self._retired_fds = set()
        self._stderr = open(dup(stderr.fileno()), 'w')
        super().__init__()

    @locked
    @initializer
    def _agent(self):
        return self._fruitbak.pool.agent()

    # latin-1 is an encoding that provides a (dummy) 1:1 byte:char mapping
    encoding = 'latin-1'

    def _fusepy_to_unicode(self, s):
        return s.encode(self.encoding).decode('UTF-8', 'surrogateescape')

    def _unicode_to_fusepy(self, s):
        return s.encode('UTF-8', 'surrogateescape').decode(self.encoding)

    def _log(self, *args):
        print(*args, file=self._stderr, flush=True)

    def _trace(self, function, *args):
        self._log(f'{function}({", ".join(map(repr, args))})')

    _next_fd = 0

    def _allocate_fd(self, obj):
        retired_fds = self._retired_fds
        try:
            fd = retired_fds.pop()
        except KeyError:
            with self.lock:
                fd = self._next_fd
                self._next_fd = fd + 1
        self._fds[fd] = obj
        return fd

    def _deallocate_fd(self, fd):
        del self._fds[fd]
        self._retired_fds.add(fd)

    _next_dev = 1

    def _dev(self, share):
        backup = share.backup
        host = backup.host
        key = host.name, backup.index, share.name
        try:
            return self._devs[key]
        except KeyError:
            pass
        with self.lock:
            dev = self._next_dev
            self._next_dev = dev + 1
            self._devs[key] = dev
        return dev

    _next_ino = 2

    def _ino(self, *key):
        try:
            return self._inos[key]
        except KeyError:
            pass
        with self.lock:
            ino = self._next_ino
            self._next_ino = ino + 1
            self._inos[key] = ino
        return ino

    @lru_cache()
    def _get_host(self, host):
        return self._fruitbak[host]

    @lru_cache()
    def _get_backup(self, host, backup):
        return self._get_host(host)[backup]

    @lru_cache()
    def _get_share(self, host, backup, share):
        return self._get_backup(host, backup)[share]

    @lru_cache()
    def _get_dentry(self, host, backup, share, path):
        return self._get_share(host, backup, share)[path]

    def _parse_path(self, path, root_func, host_func, backup_func, dentry_func):
        relpath = path.lstrip('/')
        components = relpath.split('/', 3) if relpath else []
        depth = len(components)

        try:
            if depth == 0:
                if root_func is None:
                    raise FuseOSError(ENOENT)
                return root_func()

            host = self._fusepy_to_unicode(components[0])

            if depth == 1:
                if host_func is None:
                    raise FuseOSError(ENOENT)
                return host_func(self._get_host(host))

            try:
                backup = int(components[1])
            except ValueError:
                raise FuseOSError(ENOENT)

            if depth == 2:
                if backup_func is None:
                    raise FuseOSError(ENOENT)
                return backup_func(self._get_backup(host, backup))

            if dentry_func is None:
                raise FuseOSError(ENOENT)

            share = self._fusepy_to_unicode(components[2])
            path = b'' if depth == 3 else components[3].encode(self.encoding)
            return dentry_func(self._get_dentry(host, backup, share, path))
        except (KeyError, FileNotFoundError):
            raise FuseOSError(ENOENT)

    def _open_dentry(self, dentry):
        return self._allocate_fd(FruitFuseFile(dentry=dentry))

    @windshield
    def open(self, path, flags):
        return self._parse_path(path, None, None, None, self._open_dentry)

    @windshield
    def read(self, path, size, offset, fd):
        file = self._fds[fd]
        dentry = file.dentry
        hashes = dentry.hashes
        num_hashes = len(hashes)
        file_size = dentry.size

        fbak = self._fruitbak
        chunk_size = fbak.chunk_size
        agent = self._agent

        result = []

        while size and offset < file_size:
            chunk_index, chunk_offset = divmod(offset, chunk_size)
            if file.chunk_index == chunk_index:
                chunk = file.chunk
            elif chunk_index < num_hashes:
                chunk = agent.get_chunk(hashes[chunk_index])
                file.chunk = chunk
                file.chunk_index = chunk_index
            else:
                break
            piece = chunk[chunk_offset : chunk_offset + size]
            piece_len = len(piece)
            offset += piece_len
            size -= piece_len
            result.append(piece)

        return b''.join(result)

    @windshield
    def release(self, path, fd):
        self._deallocate_fd(fd)

    def _getattr_root(self):
        return dict(st_mode=S_IFDIR | 0o555, st_nlink=2, st_ino=1)

    def _getattr_host(self, host):
        ino = self._ino(host.name)
        try:
            last_backup = host[-1]
        except:
            return dict(st_mode=S_IFDIR | 0o555, st_nlink=2, st_ino=ino)
        else:
            return dict(
                st_mode=S_IFDIR | 0o555,
                st_nlink=2,
                st_ino=ino,
                st_mtime=last_backup.start_time,
            )

    def _getattr_backup(self, backup):
        ino = self._ino(backup.host.name, backup.index)
        return dict(
            st_mode=S_IFDIR | 0o555, st_nlink=2, st_mtime=backup.start_time, st_ino=ino
        )

    def _getattr_dentry(self, dentry):
        size = dentry.size
        return dict(
            st_mode=dentry.mode,
            st_atime=dentry.mtime,
            st_ctime=dentry.mtime,
            st_mtime=dentry.mtime,
            st_size=size,
            st_blocks=(size + 511) // 512,
            st_blksize=self._fruitbak.chunk_size,
            st_uid=dentry.uid,
            st_gid=dentry.gid,
            st_ino=(self._dev(dentry.share) << 32) + dentry.inode,
        )

    @windshield
    def getattr(self, path, fd=None):
        return self._parse_path(
            path,
            self._getattr_root,
            self._getattr_host,
            self._getattr_backup,
            self._getattr_dentry,
        )

    @windshield
    def _readlink_dentry(self, dentry):
        return str(dentry.symlink, self.encoding)

    @windshield
    def readlink(self, path):
        return self._parse_path(path, None, None, None, self._readlink_dentry)

    def _readdir_root(self):
        root_attrs = self._getattr_root()
        encoding = self.encoding

        return [
            ('.', root_attrs, 0),
            ('..', root_attrs, 0),
            *(
                (
                    bytes(host.hostdir).decode(encoding),
                    self._getattr_host(host),
                    0,
                )
                for host in self._fruitbak
            ),
        ]

    def _readdir_host(self, host):
        return [
            ('.', self._getattr_host(host), 0),
            ('..', self._getattr_root(), 0),
            *(
                (
                    str(backup.index),
                    self._getattr_backup(backup),
                    0,
                )
                for backup in host
            ),
        ]

    def _readdir_backup(self, backup):
        host = backup.host
        host_name = host.name
        backup_index = backup.index
        encoding = self.encoding

        return [
            ('.', self._getattr_backup(backup), 0),
            ('..', self._getattr_host(host), 0),
            *(
                (
                    bytes(share.sharedir).decode(encoding),
                    self._getattr_dentry(
                        self._get_dentry(host_name, backup_index, share.name, b'')
                    ),
                    0,
                )
                for share in backup
            ),
        ]

    def _readdir_dentry(self, dentry):
        share = dentry.share
        backup = share.backup
        encoding = self.encoding
        name = dentry.name
        if name:
            parent_name, _, _ = name.rpartition(b'/')
            parent_dentry = self._get_dentry(
                backup.host.name, backup.index, share.name, parent_name
            )
            parent_attrs = self._getattr_dentry(parent_dentry)
        else:
            parent_attrs = self._getattr_backup(backup)

        return [
            ('.', self._getattr_dentry(dentry), 0),
            ('..', parent_attrs, 0),
            *(
                (
                    dentry.name.rpartition(b'/')[2].decode(encoding),
                    self._getattr_dentry(dentry),
                    0,
                )
                for dentry in share.ls(name)
            ),
        ]

    @windshield
    def readdir(self, path, fd):
        return self._parse_path(
            path,
            self._readdir_root,
            self._readdir_host,
            self._readdir_backup,
            self._readdir_dentry,
        )

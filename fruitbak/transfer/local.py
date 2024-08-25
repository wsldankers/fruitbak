from os import O_RDONLY, major, minor
from os.path import samestat
from pathlib import Path, PurePath
from stat import *
from sys import exc_info
from traceback import print_exc, print_exception
from typing import Optional

from fruitbak.dentry import Dentry
from fruitbak.transfer import Transfer
from fruitbak.util import Initializer, initializer, sysopen, sysopendir, xyzzy


class fruitwalk:
    _message: Optional[bool] = None

    def __init__(self, *args, **kwargs):
        self._iterator = self._walktop(*args, **kwargs)

    def __next__(self):
        message = self._message
        self._message = False
        return self._iterator.send(message)

    def __iter__(self):
        return self

    def skip(self):
        self._message = True

    @classmethod
    def _walktop(self, top='.', onerror=None, *, dir_fd=None):
        if onerror is None:
            onerror = xyzzy

        if isinstance(top, PurePath):
            path = top
            top = str(top)
        else:
            path = Path(top)

        try:
            fd = sysopendir(top, dir_fd=dir_fd, follow_symlinks=False)
        except NotADirectoryError:
            name = path.name
            try:
                fd = sysopendir(str(path.parent), dir_fd=dir_fd, follow_symlinks=False)
                st = fd.stat(name)
            except:
                onerror(*exc_info())
            else:
                yield Path(name), st, fd
        except:
            onerror(*exc_info())
        else:
            with fd:
                try:
                    st = fd.stat()
                except:
                    onerror(*exc_info())
                else:
                    path = Path()
                    skip = yield path, st, fd
                    if not skip:
                        yield from self._walkrest(fd, path, onerror)

    @classmethod
    def _walkrest(self, dir_fd, path, onerror):
        try:
            names = dir_fd.listdir()
        except:
            onerror(*exc_info())
        else:
            entries = []
            for name in names:
                try:
                    st = dir_fd.stat(name, follow_symlinks=False)
                except:
                    onerror(*exc_info())
                else:
                    entries.append((name, st))

            for name, st in entries:
                entry_path = path / name
                skip = yield entry_path, st, dir_fd
                if not skip and S_ISDIR(st.st_mode):
                    try:
                        fd = dir_fd.sysopendir(name, follow_symlinks=False)
                    except:
                        onerror(*exc_info())
                    else:
                        with fd:
                            try:
                                if not samestat(st, fd.stat()):
                                    continue
                            except:
                                onerror(*exc_info())
                            else:
                                yield from self._walkrest(fd, entry_path, onerror)


def samedentry(a, b):
    if a is None:
        return False
    if b is None:
        return False
    if a.mode != b.mode:
        return False
    if a.size != b.size:
        return False
    if a.mtime != b.mtime:
        return False
    if a.uid != b.uid:
        return False
    if a.gid != b.gid:
        return False
    return True


class LocalTransfer(Transfer):
    @initializer
    def strict_excludes(self):
        excludes = set()
        mountpoint = Path(self.mountpoint)
        for e in self.excludes:
            if not e.endswith('/'):
                p = Path(e)
                if p.is_absolute():
                    try:
                        rel = p.relative_to(mountpoint)
                    except ValueError:
                        pass
                    else:
                        excludes.add(rel)
                else:
                    excludes.add(p)
        return frozenset(excludes)

    @initializer
    def recursion_excludes(self):
        excludes = set()
        mountpoint = Path(self.mountpoint)
        for e in self.excludes:
            p = Path(e)
            if p.is_absolute():
                try:
                    rel = p.relative_to(mountpoint)
                except ValueError:
                    pass
                else:
                    excludes.add(rel)
            else:
                excludes.add(p)
        return frozenset(excludes)

    def transfer(self):
        newshare = self.newshare
        reference = self.reference
        chunk_size = self.fruitbak.chunk_size
        strict_excludes = self.strict_excludes
        recursion_excludes = self.recursion_excludes

        def normalize(path):
            return '/'.join(path.relative_to(path.anchor).parts)

        seen = {}

        one_filesystem = self.one_filesystem
        dev = None

        walk = fruitwalk(self.path, onerror=print_exception)
        for path, st, parent_fd in walk:
            if dev is None:
                dev = st.st_dev
            elif one_filesystem and dev != st.st_dev:
                walk.skip()
                continue
            if path in recursion_excludes:
                walk.skip()
            if path in strict_excludes:
                continue
            name = path.name
            path = normalize(path)
            dentry = Dentry(
                name=path,
                mode=st.st_mode,
                size=st.st_size,
                mtime=st.st_mtime_ns,
                uid=st.st_uid,
                gid=st.st_gid,
                share=reference,
            )
            ino = st.st_dev, st.st_ino
            hardlink = seen.get(ino)
            if hardlink is None:
                if not dentry.is_directory and st.st_nlink > 1:
                    seen[ino] = path
                if dentry.is_file:
                    ref_dentry = reference.get(path)
                    if samedentry(dentry, ref_dentry):
                        dentry.size = ref_dentry.size
                        dentry.hashes = ref_dentry.hashes
                    else:
                        try:
                            fd = parent_fd.sysopen(
                                name, O_RDONLY, follow_symlinks=False
                            )
                        except:
                            print_exc()
                        else:
                            with fd:
                                try:
                                    is_same = samestat(st, fd.stat())
                                except:
                                    print_exc()
                                else:
                                    if is_same:
                                        hashes = []
                                        size = 0
                                        while True:
                                            buf = fd.read(chunk_size)
                                            if not buf:
                                                break
                                            hashes.append(newshare.put_chunk(None, buf))
                                            buf_len = len(buf)
                                            size += buf_len
                                            if buf_len < chunk_size:
                                                break
                            dentry.size = size
                            dentry.hashes = hashes

                elif dentry.is_symlink:
                    try:
                        symlink = parent_fd.readlink(name)
                    except:
                        print_exc()
                    else:
                        dentry.symlink = symlink
                elif dentry.is_device:
                    dentry.rdev = major(st.st_rdev), minor(st.st_rdev)
            else:
                dentry.is_hardlink = True
                dentry.hardlink = hardlink
            newshare.add_dentry(dentry)

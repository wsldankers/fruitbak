"""Represent a previously backed up share"""

from json import load as load_json

from hardhat import Hardhat, normalize as hardhat_normalize

from fruitbak.dentry import Dentry, HardlinkDentry, dentry_layout_size
from fruitbak.util import (
    Initializer,
    ensure_byteslike,
    ensure_str,
    initializer,
    lockingclass,
    unlocked,
)


class ShareError(Exception):
    """Something Share-related went wrong."""


class NestedHardlinkError(ShareError):
    pass


class MissingLinkError(ShareError):
    pass


@lockingclass
class Share(Initializer):
    """Represent a share to back up.

    Hosts have "shares" (usually filesystems/mountpoints for Unix
    systems and drives for Windows systems) though it's perfectly
    possible to have only one "share" for the entire host if the
    distinction is not relevant/applicable for the host.

    Implements part of the `Mapping` interface:

            `share[path]`
            `share.get(path)`

    ...are supported operations for fetching a single entry as a
    `fruitbak.dentry.Dentry` object, and:

            `len(share)`

    will return the number of entries in this share."""

    @unlocked
    @initializer
    def fruitbak(self):
        """The main `Fruitbak` object. Defaults to the host's `Fruitbak` object.

        :type: fruitbak.Fruitbak"""

        return self.host.fruitbak

    @unlocked
    @initializer
    def host(self):
        """The host object that this share belongs to. Defaults to the host of
        the backup this share belongs to.

        :type: fruitbak.host.Host"""

        return self.backup.host

    @unlocked
    @initializer
    def name(self):
        """The (unencoded) name of this share. Defaults to the decoded
        name of the share directory.

        :type: str"""

        return self.fruitbak.path_to_name(self.sharedir.name)

    @unlocked
    @initializer
    def sharedir(self):
        """The (encoded) relative name of the directory containing the data
        pertinent to this share. Defaults to the encoded name of this share.

        :type: pathlib.Path"""

        return self.fruitbak.name_to_path(self.name)

    @initializer
    def sharedir_fd(self):
        """The file descriptor of the directory containing the data
        pertinent to this share. Opened on demand.

        :type: fruitbak.util.fd"""

        return self.backup.sharedir_fd.sysopendir(self.sharedir)

    @initializer
    def info(self):
        """Statistics about this share. Query using the methods below.

        :type: fruitbak.util.fd"""

        try:
            return self.backup.info['shares'][self.name]
        except KeyError:
            with open('info.json', 'r', opener=self.sharedir_fd.opener) as fp:
                return load_json(fp)

    @unlocked
    @initializer
    def start_time(self):
        """When Fruitbak started to backup this share, in nanoseconds since 1970Z.

        :type: int"""

        t = int(self.info['startTime'])
        if t < 1000000000000000000:
            return t * 1000000000
        else:
            return t

    @unlocked
    @initializer
    def end_time(self):
        """When Fruitbak finished backing up this share, in nanoseconds since 1970Z.

        :type: int"""

        t = int(self.info['endTime'])
        if t < 1000000000000000000:
            return t * 1000000000
        else:
            return t

    @unlocked
    @initializer
    def mountpoint(self):
        """The place where this share would normally be mounted on the host.

        :type: int"""

        return str(self.info['mountpoint'])

    @unlocked
    @initializer
    def path(self):
        """The place where this share was mounted on the host during the backup.

        :type: int"""

        return str(self.info['path'])

    @unlocked
    @initializer
    def error(self):
        """A description of why this share failed to backup, or `None` if it
        didn't fail. Currently unused and always `None`.

        :type: str or None"""

        try:
            return str(self.info['error'])
        except KeyError:
            return None

    @initializer
    def metadata(self):
        """File and directory metadata of this share, as a `Hardhat` object.

        :type: hardhat.Hardhat"""

        return Hardhat('metadata.hh', dir_fd=self.sharedir_fd)

    @unlocked
    def _parse_dentry(self, name, data, inode):
        """Parse a filesystem metadata entry and resolve any hardlinks.

        :param bytes name: The name of the filesystem entry.
        :param bytes data: The metadata of the filesystem entry.
        :param int inode: The (fake) inode of the filesystem entry.

        :return: A Dentry (or HardlinkDentry) object representing the metadata.
        :rtype: fruitbak.dentry.Dentry"""

        dentry = Dentry(data, name=name, inode=inode, share=self)
        if dentry.is_hardlink:
            target_name = dentry.hardlink
            c = self.metadata.find(target_name)
            target = Dentry(c.value, name=target_name, inode=c.inode, share=self)
            if target.is_hardlink:
                raise NestedHardlinkError(
                    "'%s' is a hardlink pointing to '%s', but that is also a hardlink"
                    % (ensure_str(dentry.name), ensure_str(target_name))
                )
            return HardlinkDentry(dentry, target)
        else:
            return dentry

    @unlocked
    def hashes(self):
        """Hashes for all regular files in this share.

        :return: An iterator of bytes objects that each contains zero or more
                concatenated hashes.
        :rtype: fruitbak.dentry.Dentry"""

        for data in self.metadata.values():
            d = Dentry(data)
            if d.is_file and not d.is_hardlink:
                yield d.extra

    @unlocked
    def ls(self, path=b'', parent=False, strict=True):
        """Perform a non-recursive listing of a directory in this share.

        :param bytes path: The path to list.
        :param bool parent: Whether to include the containing directory.
        :param bool strict: Whether to resolve hardlinks in such a way
                that all instances of a dentry after the first always point
                to the very first instance that was found. Uses more memory
                but may be useful depending on the circumstances. Defaults
                to `True`.

        :return: `Dentry` objects representing the items in the directory.
        :rtype: iter(fruitbak.dentry.Dentry)"""

        return self._hardlink_inverter(self.metadata.ls(path, parent=parent), strict)

    @unlocked
    def find(self, path=b'', parent=True, strict=False):
        """Perform a recursive listing of a directory in this share.

        :param bytes path: The path to list.
        :param bool parent: Whether to include the containing directory.
        :param bool strict: Whether to resolve hardlinks in such a way
                that all instances of a dentry after the first always point
                to the very first instance that was found. Uses more memory
                but may be useful depending on the circumstances. Defaults
                to `False`.

        :return: `Dentry` objects representing the items in the directory.
        :rtype: iter(fruitbak.dentry.Dentry)"""

        return self._hardlink_inverter(self.metadata.find(path, parent=parent), strict)

    @unlocked
    def _hardlink_inverter(self, c, strict):
        """Resolve hardlinks for a directory listing. When listing a
        subdirectory, hardlinks may appear in any order. The original
        file might come first, might come after being referenced or
        might not even be in the listing at all.

        In all cases this method makes sure that for any hardlink chains
        a regular file is yielded first and all subsequent entries refer
        to an earlier entry.

        :param HardlinkCursor c: The entries to invert.
        :param bool strict: Whether to resolve hardlinks in such a way
                that all instances of a dentry after the first always point
                to the very first instance that was found. Uses more memory
                but may be useful depending on the circumstances.

        :return: `Dentry` objects representing the items in the directory.
        :rtype: iter(fruitbak.dentry.Dentry)"""

        remap = {}
        first_inode = None
        metadata = self.metadata
        for name, data in c:
            inode = c.inode
            if first_inode is None:
                first_inode = inode

            dentry = Dentry(data, name=name, inode=inode, share=self)

            try:
                remapped = remap[name]
            except KeyError:
                pass
            else:
                # This is a normal entry, but it was the target of a hardlink that
                # was output earlier as if it was a regular, non-hardlink dentry.
                # So now we'll pretend that *this* was the hardlink.

                if not strict:
                    # Doing this saves memory but may generate a => b => c type hardlink
                    # chains for remapped hardlinks. Not pretty but technically still
                    # correct for many purposes.
                    del remap[name]

                target = Dentry(remapped, share=self)
                remapped_name = target.extra

                target.is_hardlink = False
                target.name = remapped_name
                target.extra = dentry.extra
                target.inode = dentry.inode

                dentry.is_hardlink = True
                dentry.hardlink = remapped_name

                yield HardlinkDentry(dentry, target)
                continue

            if dentry.is_hardlink:
                hardlink = dentry.hardlink
                try:
                    remapped = remap[hardlink]
                except KeyError:
                    pass
                else:
                    # This is a hardlink to another entry that got demoted from
                    # original hardlink target to just a hardlink. Return a hardlink
                    # that points to the new "real" file.

                    target = Dentry(metadata[hardlink], inode=inode, share=self)

                    remapped_name = remapped[dentry_layout_size:]
                    target.name = remapped_name
                    dentry.hardlink = remapped_name

                    yield HardlinkDentry(dentry, target)
                    continue

                target_cursor = metadata.ls(hardlink)
                try:
                    target_name = target_cursor.key
                    target_data = target_cursor.value
                    target_inode = target_cursor.inode
                except KeyError as e:
                    raise MissingLinkError(
                        "'%s' is a hardlink to '%s' but the latter does not exist"
                        % (ensure_str(name), ensure_str(hardlink))
                    ) from e

                target = Dentry(
                    target_data, name=target_name, inode=target_inode, share=self
                )
                if target.is_hardlink:
                    raise NestedHardlinkError(
                        "'%s' is a hardlink pointing to '%s', but that is also a hardlink"
                        % (ensure_str(name), ensure_str(target_name))
                    )

                if first_inode <= target_inode < inode:
                    # target is already output
                    yield HardlinkDentry(dentry, target)
                else:
                    # We'll pretend that this was the original file and output a hardlink later.
                    remap[target_name] = b''.join((data[:dentry_layout_size], name))

                    target.name = name
                    yield target

                continue

            yield dentry

    @unlocked
    def __bool__(self):
        return True

    @unlocked
    def __len__(self):
        return len(self.metadata)

    @unlocked
    def __getitem__(self, path):
        c = self.metadata.find(hardhat_normalize(ensure_byteslike(path)))
        if c.key is None:
            raise KeyError(path)
        return self._parse_dentry(path, c.value, c.inode)

    @unlocked
    def get(self, key, default=None):
        """Retrieve a single share entry by its path.
        Return `default` if the path was not found.

        :param bytes key: The path to retrieve.
        :param default: The value to return when the path is not found.
        :type default: fruitbak.dentry.Dentry or None

        :return: The Dentry object representing the metadata.
        :rtype: fruitbak.dentry.Dentry or None"""

        try:
            return self[key]
        except KeyError:
            return default

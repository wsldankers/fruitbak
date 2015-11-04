=encoding utf8

=head1 NAME

Fruitbak::Dentry - bookkeeping for filesystem entries inside Fruitbak

=head1 SYNOPSIS

 my $dentry = new Fruitbak::Dentry(name => 'foo/bar', mode => ...);

=head1 DESCRIPTION

Objects of this type represent entries in a filesystem, including the name,
metadata such as file size and last modification time, as well as file type
specific information such as symlink destinations or block device major
and minor numbers.

These objects are used in Fruitbak to represent filesystem entries both
when they are stored as part of the process of creating a backup, and when
listing or retrieving files in an existing backup.

These objects are used in Fruitbak to represent filesystem entries both
when they are stored as part of the process of creating a backup, and when
retrieving files 

Specific to Fruitbak is the digests information: the list of digests of the
data chunks that when concatenated form the contents of the file.

Hardlinks in Fruitbak are handled in a way that is more similar to symlinks
than the usual unix system of inode indirection. Hardlinks do not have
target file type specific information (such as digest lists) themselves; to
get that information you need to retrieve the entry using the name returned
by the hardlink function.

All metadata (such as size, file ownership, file type, etcetera) is stored
with the hardlink as it was found on the filesystem, which means it is
usually the same as the hardlink destination. It may differ if, for
example, the file was modified between backing up the hardlink and its
target.

Please note that most unix implementations allow you to create hardlinks
to not only plain files but also to block/character device nodes, named
pipes, unix domain sockets and even to symlinks. Only hardlinks to
directories are generally not possible.

Some functions that return a Fruitbak::Dentry entry may return a
Fruitbak::Dentry::Hardlink object instead, which is a convenience wrapper
that behaves more like a hardlink would on a unix filesystem: functions to
access the filetype specific data will return data from the hardlink target
instead. See the Fruitbak::Dentry::Hardlink manpage for more detail.

=cut

package Fruitbak::Dentry;

use Class::Clarity -self;

use Fcntl qw(:mode);
use Scalar::Util qw(blessed);

=head1 CONSTRUCTOR

No arguments are required by Fruitbak::Dentry itself: objects of this
class simply hold the information they're given and don't do much other
than providing access to it.

=head1 CONSTANTS

=over

=item R_HARDLINK

This bit is set in mode fields to indicate that this is a hardlink.
In contrary to real UNIX filesystems, there is no filename-to-inode
indirection, hardlinks are handled just like symlinks. Fruitbak can
get away with this simplification because its databases are write-once.

=back

=cut

use constant R_HARDLINK => 0x40000000;

=head1 FIELDS

Fruitbak makes heavy use of Class::Clarity (the base class of this class).
One of the features of Class::Clarity is ‘fields’: elements of the object
hash with eponymous getters and setters. These fields can optionally have
an initializer: a function that is called when no value is yet assigned to
the hash element. For more information, see L<Class::Clarity>.

=over

=item field name

The filesystem path of this entry. 

=cut

field name;

=item field mode

The permission and file type bits. The meaning of each bit is the same as
for the mode returned by stat().

=cut

field mode;

=item field size

The size as it was reported by the filesystem. Note that for regular files
that could not be read, the size may be non-zero even though the digest
list is empty. However, if there are digests, the size field is guaranteed
(by the transfer method) to be equal to the bytes that were actually read
from the filesystem and stored in the Fruitbak pool.

=cut

field size;

=item field mtime_ns

The last modification time, in nanoseconds since the unix epoch.

=cut

field mtime_ns;

=item field uid

The numeric user ID of the owner of this entry.

=cut

field uid;

=item field gid

The numeric group ID of the owner of this entry.

=cut

field gid;

=item field extra

The filetype specific data for this directory entry, in a binary form
suitable for storing in the metadata database. Do not set.

field extra => '';

=item field inode

The inode number for this entry, either the value as it was read from the
original filesystem or the dummy inode number that Fruitbak (or rather,
File::Hardhat) assigns to it when reading from the database. In the case
of hardlinks, the inode numbers of the hardlink and its target are not
guaranteed to be the same (or to differ, for that matter).

=cut

field inode;

=back

=head1 METHODS

=over

=item target()

For compatibility with Fruitbak::Dentry::Hardlink objects. This method
simply returns the object itself.

=cut

sub target { return $self }

=item target()

For compatibility with Fruitbak::Dentry::Hardlink objects. This method
simply returns the object itself.

=cut

sub original { return $self }

=item digests([$newvalue])

Get or set the list of digests. The (optional) argument and return value are
a simple scalar that the concatenation of the digests. Calling this
function with an argument sets the digests, calling it without an argument
retrieves them.

=cut

sub digests {
	confess("trying to treat an unreferenced hardlink as a regular file")
		if $self->is_hardlink;
	confess("attempt to access digests for something that is not a file")
		unless $self->is_file;
	return $self->extra(@_);
}

sub storedsize {
	return 0 unless $self->is_file;
	return 0 if $self->is_hardlink;
	return 0 unless $self->digests;
	return $self->size;
}

sub mtime {
	return $self->mtime_ns(int(1000000000 * shift)) if @_;
	return $self->mtime_ns / 1000000000.0;
}

sub hardlink {
	return $self->is_hardlink ? $self->extra : undef
		unless @_;
	$self->extra(shift);
	$self->mode($self->mode | R_HARDLINK);
}

sub symlink {
	confess("trying to treat a non-symlink as one")
		unless $self->is_symlink;
	confess("trying to treat an unreferenced hardlink as a symlink")
		if $self->is_hardlink;
	return $self->extra unless @_;
	$self->extra(shift);
}

sub rdev {
	return ($self->rdev_major << 32) | $self->rdev_minor unless @_;
	my $rdev = shift;
	$self->rdev_major($rdev >> 32);
	$self->rdev_minor($rdev & 0xFFFFFFFF);
}

sub rdev_minor {
	confess("trying to treat a non-device as one")
		unless $self->is_device;
	confess("trying to treat an unreferenced hardlink as a device")
		if $self->is_hardlink;
	my ($major, $minor) = unpack('LL', $self->extra);
	return $minor unless @_;
	$self->extra(pack('LL', $major // 0, shift));
}

sub rdev_major {
	confess("trying to treat a non-device as one")
		unless $self->is_device;
	confess("trying to treat an unreferenced hardlink as a device")
		if $self->is_hardlink;
	my ($major, $minor) = unpack('LL', $self->extra);
	return $major unless @_;
	$self->extra(pack('LL', shift, $minor // 0));
}

sub is_hardlink { $self->mode & R_HARDLINK }

sub is_file { S_ISREG($self->mode) }
sub is_directory { S_ISDIR($self->mode) }
sub is_symlink { S_ISLNK($self->mode) }
sub is_device { my $mode = $self->mode; return S_ISCHR($mode) || S_ISBLK($mode) }
sub is_chardev { S_ISCHR($self->mode) }
sub is_blockdev { S_ISBLK($self->mode) }
sub is_fifo { S_ISFIFO($self->mode) }
sub is_socket { S_ISSOCK($self->mode) }

sub clone {
	return blessed($self)->new(map { ($_, $self->$_) } keys %$self);
}

my @types;
@types[S_IFREG, S_IFDIR, S_IFLNK, S_IFCHR, S_IFBLK, S_IFIFO, S_IFSOCK] =
	('file', 'directory', 'symlink', 'block device', 'character device', 'named pipe', 'unix domain socket');

sub dump {
	my $mode = $self->mode;
	my $type = $types[$mode & S_IFMT] // sprintf("unknown type %d", $mode & S_IFMT);

	my @mtime = localtime($self->mtime);

	my $res = sprintf("%s:\n\ttype: %s\n\tsize: %d\n\tmode: %o\n\tuid: %d\n\tgid: %d\n\tmtime: %04d-%02d-%02d %02d:%02d:%02d\n",
		$self->name,
		$type,
		$self->size,
		$mode & 07777,
		$self->uid,
		$self->gid,
		$mtime[5] + 1900, $mtime[4] + 1, $mtime[3], $mtime[2], $mtime[1], $mtime[0]);

	$res .= "\tsymlink: ".$self->symlink."\n"
		if $self->is_symlink;

	$res .= "\thardlink: ".$self->hardlink."\n"
		if $self->is_hardlink;

	$res .= sprintf("\tdevice: %d, %d\n", $self->rdev_major, $self->rdev_minor)
		if $self->is_device;

	return $res;
}

=back

=head1 AUTHOR

Wessel Dankers <wsl@fruit.je>

=head1 COPYRIGHT

Copyright (c) 2014,2015 Wessel Dankers <wsl@fruit.je>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

=cut

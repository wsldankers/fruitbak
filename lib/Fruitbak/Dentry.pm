=encoding utf8

=head1 NAME

Fruitbak::Dentry - bookkeeping for filesystem entries

=head1 AUTHOR

Wessel Dankers <wsl@fruit.je>

=head1 COPYRIGHT

Copyright (c) 2014  Wessel Dankers <wsl@fruit.je>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

=cut

package Fruitbak::Dentry;

use Class::Clarity -self;

use Fcntl qw(:mode);
use Scalar::Util qw(blessed);

use constant R_HARDLINK => 0x40000000;

field name;
field mode;
field size;
field mtime;
field uid;
field gid;
field extra => '';
field inode;

sub target { return $self }
sub original { return $self }

sub digests {
	confess("trying to treat an unreferenced hardlink as a device")
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

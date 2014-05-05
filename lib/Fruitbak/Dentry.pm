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
field hlink => undef;
field hlink_self => undef;

sub hardlink {
	return $self->is_hardlink ? $self->extra : undef
		unless @_;
	$self->extra(shift);
	$self->mode($self->plain_mode | R_HARDLINK);
}

sub symlink {
	confess("trying to treat a non-symlink as one")
		unless $self->is_symlink;
	confess("trying treat an unreferenced hardlink as a symlink")
		if $self->is_hardlink;
	return $self->extra unless @_;
	$self->extra(shift);
}

sub rdev {
	confess("trying to treat a non-device as one")
		unless $self->is_device;
	confess("trying treat an unreferenced hardlink as a device")
		if $self->is_hardlink;
	return unpack('Q', $self->extra) unless @_;
	$self->extra(pack('Q', shift));
}

sub plain_mode { $self->mode & ~R_HARDLINK }

sub is_hardlink { $self->mode & R_HARDLINK }

sub is_file { $self->mode & S_IFREG }
sub is_directory { $self->mode & S_IFDIR }
sub is_symlink { $self->mode & S_IFLNK }
sub is_device { $self->mode & (S_IFCHR|S_IFBLK) }
sub is_chardev { $self->mode & S_IFCHR }
sub is_blockdev { $self->mode & S_IFBLK }
sub is_fifo { $self->mode & S_IFIFO }
sub is_socket { $self->mode & S_IFSOCK }

sub clone {
	return blessed($self)->new(map { ($_, $self->$_) } keys %$self);
}

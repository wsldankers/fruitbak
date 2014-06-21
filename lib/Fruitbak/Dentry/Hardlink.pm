=encoding utf8

=head1 NAME

Fruitbak::Dentry - bookkeeping for hardlinked filesystem entries

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

package Fruitbak::Dentry::Hardlink;

use Fruitbak::Dentry -self;

field target;
field original;

sub name {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->original->name;
}

sub inode {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->original->inode;
}

sub mode {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->mode;
}

sub size {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->size;
}

sub storedsize {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->storedsize;
}

sub mtime {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->mtime;
}

sub uid {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->uid;
}

sub gid {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->gid;
}

sub digests {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->digests(@_);
}

sub hardlink {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->name;
}

sub symlink {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->name;
}

sub rdev {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->rdev;
}

sub rdev_minor {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->rdev_minor;
}

sub rdev_major {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->rdev_major;
}

sub extra {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->extra;
}

use constant is_hardlink => 1;

sub is_file {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->is_file;
}

sub is_directory {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->is_directory;
}

sub is_symlink {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->is_symlink;
}

sub is_device {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->is_device;
}

sub is_chardev {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->is_chardev;
}

sub is_blockdev {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->is_blockdev;
}

sub is_fifo {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->is_fifo;
}

sub is_socket {
	confess "attempt to modify a Fruitbak::Dentry::Hardlink directly" if @_;
	return $self->target->is_socket;
}

sub clone {
	return blessed($self)->new(map { ($_, $self->$_->clone) } keys %$self);
}

sub dump {
	my $original = $self->original->dump;
	my $target = $self->target->dump;

	$original =~ s/^/\t/m;
	$target =~ s/^/\t/m;

	return "original:\n$original\ntarget:\n$target";
}

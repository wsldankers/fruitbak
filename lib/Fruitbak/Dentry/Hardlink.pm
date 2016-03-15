=encoding utf8

=head1 NAME

Fruitbak::Dentry::Hardlink - bookkeeping for hardlinked filesystem entries

=head1 SYNOPSIS

 my $hardlink = new Fruitbak::Dentry::Hardlink(
	original => $rawhardlink,
	target => $target,
 );

 print $hardlink->storedsize;

=head1 DESCRIPTION

This class provides a convenience wrapper for hardlinks and their targets.
It provides a combined read-only view on the dentries that behaves like the
target in most respects (allowing you to easily access the file contents
and metadata as it was backed up) while still having the original hardlink
name.

=cut

package Fruitbak::Dentry::Hardlink;

use Fruitbak::Dentry -self;

=head1 FIELDS

Fruitbak makes heavy use of Class::Clarity (the base class of this class).
One of the features of Class::Clarity is ‘fields’: elements of the object
hash with eponymous getters and setters. These fields can optionally have
an initializer: a function that is called when no value is yet assigned to
the hash element. For more information, see L<Class::Clarity>.

=over

=item field target

The plain Dentry that represents the target of this hardlink. This entry
will return false on ->is_hardlink.

=cut

field target;

=item field original

The original (plain) hardlink Dentry. This entry will return true on
->is_hardlink.

=cut

field original;

=back

=head1 METHODS

=over

=item name()

Returns the name of the original hardlink Dentry.

=cut

sub name {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->original->name;
}

=item inode()

=item mode()

=item size()

=item storedsize()

=item mtime_ns()

=item uid()

=item gid()

=item digests()

=item hardlink()

=item symlink()

=item rdev()

=item rdev_minor()

=item rdev_major()

=item extra()

Various (read-only) access methods that return the corresponding value of the
target Dentry.

=cut

sub inode {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->inode;
}

sub mode {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->mode;
}

sub size {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->size;
}

sub storedsize {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->storedsize;
}

sub mtime_ns {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->mtime_ns;
}

sub uid {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->uid;
}

sub gid {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->gid;
}

sub digests {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->digests(@_);
}

sub hardlink {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->name;
}

sub symlink {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->name;
}

sub rdev {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->rdev;
}

sub rdev_minor {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->rdev_minor;
}

sub rdev_major {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->rdev_major;
}

sub extra {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->extra;
}

=item is_hardlink()

Always returns true.

=cut

use constant is_hardlink => 1;

=item is_file()

=item is_directory()

=item is_symlink()

=item is_device()

=item is_chardev()

=item is_blockdev()

=item is_fifo()

=item is_socket()

Various functions that query the target Dentry.

=cut

sub is_file {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->is_file;
}

sub is_directory {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->is_directory;
}

sub is_symlink {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->is_symlink;
}

sub is_device {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->is_device;
}

sub is_chardev {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->is_chardev;
}

sub is_blockdev {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->is_blockdev;
}

sub is_fifo {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->is_fifo;
}

sub is_socket {
	confess("attempt to modify a Fruitbak::Dentry::Hardlink directly") if @_;
	return $self->target->is_socket;
}

=item clone()

Returns an independent clone of this object. The referenced original and
target Dentry will also be clones.

=cut

sub clone {
	return blessed($self)->new(map { ($_, $self->$_->clone) } keys %$self);
}

=item dump()

Returns a human-readable string that can be used for debugging the contents
of both the original and target Dentry. Do not depend on the format, it may
change without notice.

=cut

sub dump {
	my $original = $self->original->dump;
	my $target = $self->target->dump;

	$original =~ s/^/\t/m;
	$target =~ s/^/\t/m;

	return "original:\n$original\ntarget:\n$target";
}

=back

=head1 AUTHOR

Wessel Dankers <wsl@fruit.je>

=head1 COPYRIGHT

Copyright (c) 2014,2016 Wessel Dankers <wsl@fruit.je>

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

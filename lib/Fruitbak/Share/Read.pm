=encoding utf8

=head1 NAME

Fruitbak::Share::Read - reads a share of a completed backup

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

package Fruitbak::Share::Read;

use strict;
use warnings;

use Fcntl qw(:mode);
use Hardhat;
use Fruitbak::Share::Cursor;
use Fruitbak::Share::Format;
use Fruitbak::Pool::Read;
use Fruitbak::Dentry;
use Fruitbak::Dentry::Hardlink;

use Class::Clarity -self;

field dir => sub { $self->backup->sharedir . '/' . mangle($self->name) };
field hh => sub { new Hardhat($self->dir . '/metadata.hh') };
field backup;
field fbak => sub { $self->backup->fbak };

# directory listing, returns a list of string
sub ls {
	my $path = shift;
	my $c = $self->hh->ls($path);
	return new Fruitbak::Share::Cursor(share => $self, hhcursor => $c)
		unless wantarray;
	my @res;
	for(;;) {
		my $name = $c->fetch;
		last unless defined $name;
		push @res, $name;
	}
	return @res;
}

# recursive directory listing, returns a list of string
sub find {
	my $path = shift;
	my $c = $self->hh->find($path);
	return new Fruitbak::Share::Cursor(share => $self, hhcursor => $c)
		unless wantarray;
	my @res;
	for(;;) {
		my $name = $c->fetch;
		last unless defined $name;
		push @res, $name;
	}
	return @res;
}

# given a name, return a Fruitbak::Dentry
sub get_entry {
	my $path = shift;
	my $hh = $self->hh;
	my ($name, $data, $inode) = $hh->get($path)
		or return;
	my $dentry = attrparse($data, name => $name, inode => $inode);

	if($dentry->is_hardlink) {
		($name, $data, $inode) = $hh->get($dentry->hardlink);
		my $target = attrparse($data, name => $name, inode => $inode);
		return new Fruitbak::Dentry::Hardlink(original => $dentry, target => $target);
	}

	return $dentry;
}

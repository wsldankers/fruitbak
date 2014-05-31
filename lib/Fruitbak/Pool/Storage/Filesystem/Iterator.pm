=encoding utf8

=head1 NAME

Fruitbak::Pool::Storage::Filesystem::Iterator - list fs pool contents

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

package Fruitbak::Pool::Storage::Filesystem::Iterator;

use autodie;

use Fruitbak::Pool::Iterator -self;

use IO::Dir;
use MIME::Base64;

field dir => sub { $self->storage->dir };
field subdirs => sub {
	my $dir = $self->dir;
	my $fh = new IO::Dir($dir)
		or die "can't open $dir: $!\n";
	my $storage = $self->storage;
	return [grep { /^[^.].$/ } $fh->read];
};

sub fetch {
	my $subdirs = $self->subdirs;
	my $storage = $self->storage;
	my $dir = $self->dir;
	while(@$subdirs) {
		my $subdir = shift @$subdirs;
		my $fh = new IO::Dir("$dir/$subdir")
			or die "can't open $dir/$subdir: $!\n";
		my @entries =
			map { $storage->path2digest("$subdir/$_") }
			grep { !/^\./ }
			$fh->read;
		return \@entries if @entries;
	}
	$self->subdirs_reset;
	return;
}

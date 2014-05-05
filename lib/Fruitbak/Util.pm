=encoding utf8

=head1 NAME

Fruitbak::Util - class for miscellaneous utility functions

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

package Fruitbak::Util;

use strict;
use warnings FATAL => 'all';

use Exporter qw(import);

our @EXPORT = qw(normalize_and_check_directory);

sub normalize_directory {
	local $_ = shift;
	return undef unless defined;
	s{//+}{/}ga;
	s{/\K\.$}{}a;
	s{.\K/$}{}a;
	s{^\./}{}a;
	s{/\./}{/}ga;
	# again, because it may overlap:
	s{/\./}{/}ga;
	return $_;
}

sub check_directory {
	my $dir = shift;
	die "directory not defined\n" unless defined $dir;
	die "directory '$dir' does not exist\n" unless -e $dir;
	die "'$dir' is not a directory\n" unless -d $dir;
	return $dir;
}

sub normalize_and_check_directory {
	return check_directory(normalize_directory(shift));
}

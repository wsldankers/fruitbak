=encoding utf8

=head1 NAME

Fruitbak::Config::File - context package for reading Fruitbak configuration

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

package Fruitbak::Config::File;

use strict;
use warnings FATAL => 'all';

our %conf;

sub include {
	die "include(): missing argument\n"
		unless @_;
	my $file = shift;
	die "include(): undefined argument\n"
		unless defined $file;
	# make relative paths absolute
	$file =~ s{^(?!/)}{$conf{confdir}/}a;
	unless(do $file) {
		die "parsing $file: $@" if $@;
		die "reading $file: $!\n" if $!;
		die "error loading $file\n";
	}
}

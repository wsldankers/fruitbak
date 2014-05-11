=encoding utf8

=head1 NAME

Fruitbak::Share::Format - format and parse entries of the share database

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

package Fruitbak::Share::Format;

use strict;
use warnings;

use constant ATTRLEN => 28;
use constant MAXNAMELEN => 65535;

use Encode;
use Fruitbak::Dentry;
use Exporter qw(import);

our @EXPORT_OK = qw(ATTRLEN MAXNAMELEN attrformat attrparse mangle unmangle);
our @EXPORT = @EXPORT_OK;

# serialize attributes
sub attrformat {
	my $dentry = shift;
	return pack('QQLLLa*', map { $dentry->$_ } qw(size mtime mode uid gid extra));
}

# parse attributes
sub attrparse {
	my %attrs;
	@attrs{qw(size mtime mode uid gid extra)} = unpack('QQLLLa*', shift);
	return new Fruitbak::Dentry(%attrs);
}

sub mangle {
	my $name = shift;
	Encode::_utf8_off($name);
	return $name =~ s{[%:\\/\s.]}{sprintf("%%%02X", ord($&))}egair;
}

sub unmangle {
	my $name = shift;
	$name =~ s{\%([a-f0-9]{2})}{chr(hex($1))}egai;
	Encode::_utf8_on($name);
	Encode::_utf8_off($name) unless utf8::valid($name);
	return $name;
}
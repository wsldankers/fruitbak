=encoding utf8

=head1 NAME

Fruitbak::Share::Format - format and parse entries of the share database

=head1 AUTHOR

Wessel Dankers <wsl@fruit.je>

=head1 COPYRIGHT

Copyright (c) 2014 Wessel Dankers <wsl@fruit.je>

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

package Fruitbak::Share::Format;

use strict;
use warnings FATAL => 'all';

use constant MAXNAMELEN => 65535;
use constant FORMAT_FLAG_HARDLINK => 0x1;
use constant FORMAT_MASK => FORMAT_FLAG_HARDLINK;

use Encode;
use Fcntl qw(:mode);
use Fruitbak::Dentry;
use Exporter qw(import);

our @EXPORT_OK = qw(ATTRLEN MAXNAMELEN attrformat attrparse mangle unmangle just_the_hashes);
our @EXPORT = @EXPORT_OK;

# serialize attributes
sub attrformat {
	my $dentry = shift;
	my $flags = $dentry->is_hardlink ? FORMAT_FLAG_HARDLINK : 0;
	return pack('L<L<Q<Q<L<L<a*', $flags, map { $dentry->$_ } qw(mode size mtime_ns uid gid extra));
}

# parse attributes
sub attrparse {
	my %attrs;
	@attrs{qw(flags mode size mtime_ns uid gid extra)} = unpack('L<L<Q<Q<L<L<a*', shift);
	my $flags = delete $attrs{flags};
	if($flags & ~FORMAT_MASK) {
		my $hex = sprintf('0x%x', $flags & ~FORMAT_MASK);
		die "unknown format flags in $hex\n";
	}
	$attrs{is_hardlink} = 1 if $flags & FORMAT_FLAG_HARDLINK;
	return new Fruitbak::Dentry(%attrs, @_);
}

# extract just the hashes. returns an empty string for entries that have no hashes.
sub just_the_hashes {
	my ($flags, $mode, undef, undef, undef, undef, $extra) = unpack('L<L<Q<Q<L<L<a*', $_[0]);
	if($flags & ~FORMAT_MASK) {
		my $hex = sprintf('0x%x', $flags & ~FORMAT_MASK);
		die "unknown format flags in $hex\n";
	}
	return '' if $flags & FORMAT_FLAG_HARDLINK;
	return '' unless S_ISREG($mode);
	return $extra;
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

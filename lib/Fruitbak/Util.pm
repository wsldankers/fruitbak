=encoding utf8

=head1 NAME

Fruitbak::Util - class for miscellaneous utility functions

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

package Fruitbak::Util;

use strict;
use warnings FATAL => 'all';

use Exporter qw(import);
use Encode;

our @EXPORT = qw(normalize_and_check_directory normalize_path check_directory split_path utf8_testandset_inplace utf8_testandset utf8_disable_inplace utf8_disable parse_interval parse_size parse_bool);
our @EXPORT_OK = @EXPORT;

sub normalize_path {
	local $_ = shift;
	return undef unless defined;
	s{//+}{/}ga;
	s{/\K(?:\./)+}{}ga;
	s{/\K\.\z}{}a;
	s{.\K/\z}{}a;
	s{^(?:\./)+}{}a;
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
	return check_directory(normalize_path(shift));
}

sub split_path {
	my $path = shift;
	my @path = grep { $_ ne '.' } split(qr{/+}, $path);
	@path = ('') if !@path && $path =~ m{^/};
	return @path;
}

sub utf8_testandset_inplace {
	foreach my $val (@_) {
		# upgrade to UTF-8 if possible, without changing any bytes
		Encode::_utf8_on($val);
		Encode::_utf8_off($val) unless utf8::valid($val);
	}
	return;
}

sub utf8_testandset {
	my $str = shift;
	return undef unless defined $str;
	utf8_testandset_inplace($str);
	return $str;
}

sub utf8_disable_inplace {
	foreach my $val (@_) {
		Encode::_utf8_off($val);
	}
	return;
}

sub utf8_disable {
	my $str = shift;
	return undef unless defined $str;
	Encode::_utf8_off($str);
	return $str;
}

{
	my %units = (
		'' => 1,
		ns => 0.000000001,
		us => 0.000001,
		ms => 0.001,
		s => 1,
		m => 60,
		h => 3600,
		d => 86400,
		w => 604800,
		l => 31 * 86400,
		q => 92 * 86400,
		y => int(365.2425 * 86400),
	);

	sub parse_interval {
		my ($time) = @_;
		return undef unless defined $time;
		$time =~ /^(\d+)\s*(\w*)$/i;
		my ($scalar, $unit) = ($1, $2);
		die "can't parse time value '$time'\n"
			unless defined $scalar;
		my $multiplier = $units{lc $unit}
			or die "unknown unit $unit\n";
		return $scalar * $multiplier;
	}
}

{
	my %units = (
		'' => 1,
		k => 1024,
		m => 1048576,
		g => 1073741824,
		t => 1099511627776,
		p => 1125899906842624,
		e => 1152921504606846976,
		z => 1180591620717411303424,
		y => 1208925819614629174706176,
	);

	sub parse_size {
		my ($size) = @_;
		return undef unless defined $size;
		$size =~ /^(\d+)\s*([a-z]?)$/i
			or die "can't parse size value '$size'\n";
		my ($bytes, $unit) = ($1, $2);
		my $multiplier = $units{lc $unit}
			or die "unknown unit $unit\n";
		return $bytes * $multiplier;
	}
}

sub parse_bool {
	local $_ = shift;
	return undef unless defined;
	return 1 if /^(?:1|y|yes|true|on|enabled?)$/i;
	return 0 if /^(?:0|n|no|false|off|disabled?)$/i;
	die "unknown boolean value '$_'\n";
}

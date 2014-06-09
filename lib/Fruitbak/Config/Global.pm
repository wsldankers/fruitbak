=encoding utf8

=head1 NAME

Fruitbak::Config::Global - class for reading and accessing global Fruitbak configuration

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

package Fruitbak::Config::Global;

use Class::Clarity -self;

use Scalar::Util qw(reftype);

use Fruitbak::Host;
use Fruitbak::Config;
use Fruitbak::Config::File;

field fbak;
field cfg;
field dir => sub { $self->cfg->dir };

field data => sub {
	my $dir = $self->dir;
	my $file = "$dir/global.pl";
	my $conf = eval "package Fruitbak::Config::File; local our %conf = (confdir => \$dir); include(\$file); {%conf}";
	die $@ if $@;
	return $conf;
};

sub get_value {
	my $name = shift;
	my $data = $self->data;
	my $value = $data->{$name};
	my $type = reftype($value);
	return $value unless defined $type && $type eq 'CODE';
	local %Fruitbak::Config::File::conf = %$data;
	$value = $value->();
	%$data = (%Fruitbak::Config::File::conf, $name => $value);
	return $value;
}

sub DESTROY {} # don't try to autoload this

sub AUTOLOAD {
	my $sub = our $AUTOLOAD;
	my $off = rindex($sub, '::');
	confess("no package name in '$sub'")
		if $off == -1;
	my $pkg = substr($sub, 0, $off + 2, '');
	confess("Can't locate object method \"$sub\" via package \"$pkg\"") if @_;
	my $code = "sub $sub { my \$self = shift; return \$self->get_value(\$sub) }";
	my $err = do { local $@; eval $code; $@ };
	confess($err) if $err;
	return $self->get_value($sub);
}

=encoding utf8

=head1 NAME

Fruitbak::Config::Host - class for reading and accessing per-host Fruitbak configuration

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

package Fruitbak::Config::Host;

use Class::Clarity -self;

use Fruitbak::Host;
use Fruitbak::Config;

field fbak;
field cfg;
field dir => sub { $self->cfg->dir };
field name;

field data => sub {
	my $dir = $self->dir;
	my $name = $self->name;
	my $file = "$dir/$name.pl";
	my $commonfile = "$dir/common.pl";
	my $pkgname = $name;
	$pkgname =~ tr/-/_/;
	my $code = Fruitbak::Config::include_code();
	my $conf = eval "package Fruitbak::Config::Host::$pkgname; our %conf = (name => \$name); *include = sub { $code }; include(\$commonfile); include(\$file) if -e \$file; \\%conf";
	die $@ if $@;
	return $conf;
};

sub DESTROY {} # don't try to autoload this

sub AUTOLOAD {
	my $sub = our $AUTOLOAD;
	my $off = rindex($sub, '::');
	confess("no package name in '$sub'")
		if $off == -1;
	my $pkg = substr($sub, 0, $off + 2, '');
	confess("Can't locate object method \"$sub\" via package \"$pkg\"") if @_;
	my $code = "sub $sub { my \$self = shift; return \$self->data->{\$sub} }";
	my $err = do { local $@; eval $code; $@ };
	confess($err) if $err;
	return $self->data->{$sub};
}

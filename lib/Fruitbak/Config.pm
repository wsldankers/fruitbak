=encoding utf8

=head1 NAME

Fruitbak::Config - class for reading and accessing Fruitbak configuration

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

package Fruitbak::Config;

use Class::Clarity -self;

use Fruitbak::Config::Global;
use Fruitbak::Config::Host;
use Fruitbak::Host;

field fbak;
field dir => sub { $self->fbak->confdir };
field hostdir => sub { $self->dir . '/host' };
field global => sub { new Fruitbak::Config::Global(fbak => $self->fbak, cfg => $self) };

sub hosts {
	my $hostdir = $self->hostdir;
	my $fh = new IO::Dir($hostdir)
		or die "open($hostdir): $!\n";
	my @hosts =
		sort
		grep { Fruitbak::Host::is_valid_name($_) }
		map { s/\.pl$//a ? ($_) : () }
		$fh->read;
	return \@hosts;
}

# given a name, return a Fruitbak::Config::Host object
sub get_host {
	return new Fruitbak::Config::Host(fbak => $self->fbak, cfg => $self, name => @_);
}

use constant include_code => <<'EOT';
	die "include(): missing argument\n"
		unless @_;
	my $file = shift;
	die "include(): undefined argument\n"
		unless defined $file;
	unless(do $file) {
		die "parsing $file: $@" if $@;
		die "reading $file: $!\n" if $!;
		die "error loading $file\n";
	}
EOT

sub DESTROY {} # don't try to autoload this

sub AUTOLOAD {
	my $sub = our $AUTOLOAD;
	my $off = rindex($sub, '::');
	confess("no package name in '$sub'")
		if $off == -1;
	my $pkg = substr($sub, 0, $off + 2, '');
	my $code = "sub $sub { my \$self = shift; \$self->global->$sub(\@_) }";
	my $err = do { local $@; eval $code; $@ };
	confess($err) if $err;
	return $self->global->$sub(@_);
}

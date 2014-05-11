=encoding utf8

=head1 NAME

Fruitbak - main class that ties everything together

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

package Fruitbak;

use Class::Clarity -self;

use IO::Dir;
use Fruitbak::Util;
use Fruitbak::Config;
use Fruitbak::Pool;

field confdir;
field rootdir => sub { normalize_and_check_directory($self->cfg->rootdir) };
field hostdir => sub { normalize_and_check_directory($self->cfg->hostdir // $self->rootdir . '/host') };

weakfield cfg => sub { new Fruitbak::Config(fbak => $self) };
weakfield pool => sub { new Fruitbak::Pool(fbak => $self) };

field hosts => sub {
	my %hosts;

	# first, read whatever hosts are defined in the configuration
	@hosts{@{$self->cfg->hosts}} = ();

	# second, the hosts that exist on the filesystem
	my $hostdir = $self->hostdir;
	my $fh = new IO::Dir($hostdir)
		or die "open($hostdir): $!\n";
	my @hosts =
		sort
		grep { Fruitbak::Host::is_valid_name($_) }
		$fh->read;

	# combine the two
	@hosts{@hosts} = ();
	return [keys %hosts];
};

sub get_host {
	return new Fruitbak::Host(fbak => $self, name => @_);
}

=encoding utf8

=head1 NAME

Fruitbak::Transfer::Rsync - transfer files using rsync

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

package Fruitbak::Transfer::Rsync;

use File::RsyncP;
use Fruitbak::Transfer::Rsync::IO;

use Class::Clarity -self;

field fbak => sub { $self->host->fbak };
field host => sub { $self->share->host };
field share;

sub recv {
	my $rs = new File::RsyncP({
		logLevel => 2,
		rsyncCmd => ['rsync'],
		rsyncArgs => [qw(
			--numeric-ids
			--perms
			--owner
			--group
			--devices
			--links
			--recursive
			--hard-links
			--times
			--specials
		)],
		fio => new Fruitbak::Transfer::Rsync::IO(xfer => $self),
	});

	die "REMOVE BEFORE FLIGHT"
		if $self->share->name eq '/';

	eval {
		$rs->remoteStart(1, $self->share->name);
		$rs->go('/DUMMY');
		$rs->serverClose;
	};
	if(my $err = $@) {
		eval { $rs->abort };
		warn $@ if $@;
		die $err;
	}
}

=encoding utf8

=head1 NAME

Fruitbak::Host::Expiry::Status - expiry policy that expires by status

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

package Fruitbak::Host::Expiry::Status;

use Fruitbak::Host::Expiry -self;

field in => sub {
	my $in = $self->cfg->{in};
	die "no 'in' parameter configured for 'status' expiry policy\n"
		unless defined $in;
	$in = [$in] unless ref $in;
	my %in; @in{@$in} = ();
	return \%in;
};

sub expired {
	my $host = $self->host;
	my $backups = $host->backups;
	my $in = $self->in;
	return [grep { exists $in->{$host->get_backup($_)->status} } @$backups];
}

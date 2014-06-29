=encoding utf8

=head1 NAME

Fruitbak::Expiry::And - logical “and” operator for policies

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

package Fruitbak::Expiry::And;

use Fruitbak::Expiry -self;

field subpols => sub {
	my $host = $self->host;
	my $all = $self->cfg->{all};
	die "no 'all' set configured for 'and' expiry policy\n"
		unless defined $all;
	return [map { $host->instantiate_expiry($_) } @$all];
};

sub expired {
	my $subpols = $self->subpols;
	my $all = $self->host->backups;
	my %all; @all{@$all} = ();
	my %remaining; @remaining{@$all} = ();
	foreach my $p (@$subpols) {
		my $e = $p->expired;
		my %inverse = %remaining;
		delete @inverse{@$e};
		delete @remaining{keys %inverse};
	}
	return [sort { $a <=> $b } map { int($_) } keys %remaining];
}

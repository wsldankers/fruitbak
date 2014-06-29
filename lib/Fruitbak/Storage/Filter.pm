=encoding utf8

=head1 NAME

Fruitbak::Storage::Filter - base class for storage filters

=head1 METHODS

=head2 apply($hash, $data)

Apply the filter to the data (a scalar ref) and return the modified
data (also as a scalar ref).

=head2 unapply($hash, $data)

Like apply, but should remove the data transformation.

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

package Fruitbak::Storage::Filter;

use Fruitbak::Storage -self;

field subpool => sub {
	return $self->pool->instantiate_storage($self->cfg->{pool} // ['filesystem']);
};

sub store {
	my ($hash, $data) = @_;
	$data = $self->apply($hash, $data);
	return $self->subpool->store($hash, $data);
}
sub retrieve {
	my $hash = shift;
	my $data = $self->subpool->retrieve($hash);
	return $self->unapply($hash, $data);
}
sub has { return $self->subpool->has(@_) }
sub remove { return $self->subpool->remove(@_) }
sub iterator { return $self->subpool->iterator(@_) }

sub apply {
	return $_[1];
}

sub unapply {
	return $_[1];
}

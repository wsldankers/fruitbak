=encoding utf8

=head1 NAME

Fruitbak::Storage - store and retrieve chunks (abstract superclass)

=head1 METHODS

=head2 store(hash, dataref)

Store a chunk. First argument is the (binary) hash of the object, second
is a reference to the actual data. It is not considered an error if the
data already exists (but it is left undefined whether the existing data
is replaced).

=head2 retrieve(hash)

Return a reference to the data belonging to the supplied (binary) hash.
It is not considered an error if the hash does not exist (undef is returned
in that case).

=head2 exists(hash)

Checks if the supplied (binary) hash exists in storage.

=head2 delete(hash)

Removes the supplied (binary) hash exists in storage. It is not considered
an error if the hash does not exist.

=head1 ERROR HANDLING

All methods die() on any failure encountered.

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

package Fruitbak::Storage;

use Class::Clarity -self;

weakfield pool;
weakfield fbak => sub { $self->pool->fbak };
field cfg;

stub store;
stub retrieve;
stub has;
stub remove;
stub iterator;
stub queue;

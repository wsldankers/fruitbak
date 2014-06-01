=encoding utf8

=head1 NAME

Fruitbak::Pool::Storage::Verify - check pool data as it is retrieved

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

package Fruitbak::Pool::Storage::Verify;

use Fruitbak::Pool::Storage::Filter -self;

use MIME::Base64;

field hashalgo => sub { $self->pool->hashalgo };

sub unapply {
	my ($hash, $data) = @_;
	my $calc = $self->hashalgo->($$data);
	if($calc ne $hash) {
		my $hash64 = encode_base64($hash);
		my $calc64 = encode_base64($calc);
		die "invalid checksum ($calc64) on chunk (expected $hash64)\n";
	}
	return $data;
}

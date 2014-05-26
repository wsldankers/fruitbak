=encoding utf8

=head1 NAME

Fruitbak::Command::GC - implementation of CLI gc command

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

package Fruitbak::Command::GC;

use autodie;
no utf8;

use Fruitbak::Command -self;

use MIME::Base64;

BEGIN {
	$Fruitbak::Command::commands{gc} = [__PACKAGE__, "Clean up unused pool chunks"];
}

sub run {
	my (undef, $dummy) = @_;

	die "usage: fruitbak gc\n"
		if defined $dummy;

	my $fbak = $self->fbak;
	my $pool = $fbak->pool;
	my $iterator = $pool->iterator;

	while(my $chunks = $iterator->fetch) {
		foreach my $chunk (@$chunks) {
			print encode_base64($chunk);
		}
	}

	return 0;
}

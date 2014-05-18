=encoding utf8

=head1 NAME

Fruitbak::Pool - represents and provides access to the pool subsystem

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

package Fruitbak::Pool;

use Class::Clarity -self;

use Digest::SHA;
use MIME::Base64;

use Fruitbak::Pool::Read;
use Fruitbak::Pool::Write;
use Fruitbak::Util;

field fbak;
field cfg => sub { $self->fbak->cfg };
field hashalgo => sub { \&Digest::SHA::sha256 };
field hashsize => sub { length($self->hashalgo->('')) };
field chunksize => sub { $self->cfg->chunksize // 2097152 };
field pooldir => sub { normalize_and_check_directory($self->cfg->pooldir // $self->fbak->rootdir . '/pool') };

sub reader {
	return new Fruitbak::Pool::Read(pool => $self, @_);
}

sub writer {
	return new Fruitbak::Pool::Write(pool => $self, @_);
}

sub digest2path {
    my $digest = shift;

	my $b64 = encode_base64($digest, '');
	$b64 =~ tr{/}{_};
	$b64 =~ s{=+$}{}a;
	$b64 =~ s{^(..)}{}a;

    return "$1/$b64";
}

sub path2digest {
    my $path = shift;
	$path =~ tr{_/}{/}d;
	return decode_base64(shift =~ tr{_/}{/}dr);
}

sub digestlist {
	my $hashsize = $self->hashsize;
	my @hashes = map { encode_base64($_, '')."\n" } unpack("(a$hashsize)*", shift);
	return join('', @hashes);
}

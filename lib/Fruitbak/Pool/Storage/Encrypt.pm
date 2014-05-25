=encoding utf8

=head1 NAME

Fruitbak::Pool::Storage::Encrypt - allow for pooled data to be encrypted

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

package Fruitbak::Pool::Storage::Encrypt;

use Crypt::Rijndael;
use Crypt::OpenSSL::Random;
use Digest::SHA qw(sha256 hmac_sha256);
use IO::File;

use Fruitbak::Pool::Storage::Filter -self;

field key => sub { $self->cfg->{key} // die "no key configured for encryption plugin\n" };

# yes, this is a crappy key expansion algorithm.
# we work around this in two ways: use a random first block
# and an IV that is different for each hash
field expanded_key => sub { sha256($self->key) };

sub random_bytes() {
	unless(Crypt::OpenSSL::Random::random_status()) {
		my $fh = new IO::File('<', '/dev/urandom')
			or die "can't open /dev/urandom: $!\n";
		do {
			$fh->read(my $buf, 16)
				or die "can't read from /dev/urandom: $!\n";
			Crypt::OpenSSL::Random::random_seed($buf);
		} until Crypt::OpenSSL::Random::random_status();
	}
	return Crypt::OpenSSL::Random::random_bytes(shift);
}

sub apply {
	my ($hash, $data) = @_;

	my $aes = new Crypt::Rijndael($self->expanded_key, Crypt::Rijndael::MODE_CBC);
	my $iv = hmac_sha256($hash, $self->key);
	$aes->set_iv(substr($iv, 0, 16));
	my $len = length($$data);

	# Because the byte that indicates the padding length is part
	# of the first block, we must make it semi-random. Take an unused
	# byte from $iv and xor it with the number of padding bytes.
	my $pad = -$len & 15;
	my ($padbyte) = unpack('C', substr($iv, 16, 1));
	my $padxor = $pad ^ $padbyte;

	my $header = pack('C', $padxor).random_bytes(15 + $pad);
	return \($aes->encrypt($header.$$data));
}

sub unapply {
	my ($hash, $encrypted) = @_;

	my $aes = new Crypt::Rijndael($self->expanded_key, Crypt::Rijndael::MODE_CBC);
	my $iv = hmac_sha256($hash, $self->key);
	$aes->set_iv(substr($iv, 0, 16));
	my ($padbyte) = unpack('C', substr($iv, 16, 1));

	my $data = $aes->decrypt($$encrypted);
	my ($padxor) = unpack('C', $data);
	my $pad = $padxor ^ $padbyte;
	substr($data, 0, 16 + $pad, '');

	return \$data;
}

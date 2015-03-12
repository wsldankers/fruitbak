=encoding utf8

=head1 NAME

Fruitbak::Storage::Encrypt - allow for pooled data to be encrypted

=head1 AUTHOR

Wessel Dankers <wsl@fruit.je>

=head1 COPYRIGHT

Copyright (c) 2014 Wessel Dankers <wsl@fruit.je>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

=cut

package Fruitbak::Storage::Encrypt;

use Fruitbak::Storage -self;

use Crypt::Rijndael;
use Crypt::OpenSSL::Random;
use Digest::SHA qw(hmac_sha512);
use MIME::Base64;
use IO::File;

use Fruitbak::Storage::Encrypt::Iterator;

field hashsize => sub { $self->pool->hashsize };

sub random_bytes {
	my ($num, $force) = @_;
	return '' if defined $num && $num == 0;
	if($force || !Crypt::OpenSSL::Random::random_status()) {
		my $fh = new IO::File('/dev/urandom', '<')
			or die "can't open /dev/urandom: $!\n";
		do {
			$fh->read(my $buf, $force ? $num // 16 : 16)
				or die "can't read from /dev/urandom: $!\n";
			Crypt::OpenSSL::Random::random_seed($buf);
		} until Crypt::OpenSSL::Random::random_status();
	}
	return Crypt::OpenSSL::Random::random_bytes($num);
}

field key => sub {
	my $key = $self->cfg->{key};
	local $@;
	eval {
		die "no key configured for encryption plugin\n"
			unless defined $key;
		if($key =~ /^[A-Za-z0-9+\/]{43}=$/a) {
			$key = decode_base64($key);
		} else {
			die "encryption key must be randomly generated data\n"
				if utf8::is_utf8($key);
			die "encryption key must be 32 bytes of randomly generated data\n"
				unless length($key) == 32;
		}
		die "encryption key must be randomly generated data\n"
			if $key !~ /[^ -~]/a;
	};
	if(my $err = $@) {
		if(my $suggestion = eval { encode_base64($self->random_bytes(32, 1), '') }) {
			die $err."suggestion for a proper key: '$suggestion'\n";
		} else {
			warn $@;
			die $err;
		}
	}
	return $key;
};

field aes => sub { new Crypt::Rijndael($self->key, Crypt::Rijndael::MODE_CBC) };
field aes_iv => sub { new Crypt::Rijndael($self->key, Crypt::Rijndael::MODE_CBC) };

sub encrypt_hash {
	my $hash = shift;
	die "configured hash size must be a multiple of 16 bytes (128 bits)\n"
		if length($hash) % 16;
	return $self->aes->encrypt($hash);
}

sub decrypt_hash {
	my $hash = shift;
	die "invalid encrypted data\n"
		if length($hash) != $self->hashsize;
	return $self->aes->decrypt($hash);
}

sub encrypt_data {
	my $data = shift;
	my $len = length($$data) + 1;
	my $pad = -$len & 15;

	my $aes = $self->aes_iv;
	my $iv = $self->random_bytes(16);
	$aes->set_iv($iv);

	my $buf = pack('C', $pad).$self->random_bytes($pad).$$data;
	$buf = hmac_sha512($buf, $self->key).$buf;

	my $ciphertext = $iv.$aes->encrypt($buf);
	return \$ciphertext;
}

sub decrypt_data {
	my $data = shift;

	my $len = length($$data);
	die "invalid encrypted data\n"
		if $len % 16 || $len < 96;

	my $aes = $self->aes_iv;
	my $iv = substr($$data, 0, 16);
	$aes->set_iv(substr($iv, 0, 16));

	my $buf = $aes->decrypt(substr($$data, 16));

	die "invalid encrypted data\n"
		unless hmac_sha512(substr($buf, 64), $self->key) eq substr($buf, 0, 64);
	my $pad = unpack('C', substr($buf, 64, 1));

	my $plaintext = substr($buf, 64 + 1 + $pad);
	return \$plaintext;
}

field subpool => sub {
	return $self->pool->instantiate_storage($self->cfg->{pool} // ['filesystem']);
};

sub store {
	my $hash = shift;
	my $data = shift;
	return $self->subpool->store($self->encrypt_hash($hash), $self->encrypt_data($data), @_);
}

sub retrieve {
	my $hash = shift;
	return $self->decrypt_data($self->subpool->retrieve($self->encrypt_hash($hash), @_));
}

sub has {
	my $hash = shift;
	return $self->subpool->has($self->encrypt_hash($hash), @_);
}

sub remove {
	my $hash = shift;
	return $self->subpool->remove($self->encrypt_hash($hash), @_);
}

sub iterator {
	return new Fruitbak::Storage::Encrypt::Iterator(
		storage => $self,
		subiterator => $self->subpool->iterator(@_),
	);
}

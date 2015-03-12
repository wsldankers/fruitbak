=encoding utf8

=head1 NAME

Fruitbak::Pool::Read - read file contents from the pooling system

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

package Fruitbak::Pool::Read;

use Class::Clarity -self;

use Digest::SHA qw(sha512);

field fbak => sub { $self->pool->fbak };
field pool;
field off => 0;
field curchunkbuf;
field curchunknum => -1;
field hashsize => sub { $self->pool->hashsize };
field chunksize => sub { $self->pool->chunksize };

sub digests {
	return $self->{digests} = ref $_[0] ? $_[0] : \($_[0])
		if @_;
	confess("field 'digests' used uninitialized") unless exists $self->{digests};
	return $self->{digests};
}

sub new() {
	my $self = super;
	my $digests = $self->digests;
	my $hashsize = $self->hashsize;
	confess "invalid digests length"
		if length($$digests) % $hashsize;
	return $self;
}

sub pread {
	my ($off, $len) = @_;

	my $chunksize = $self->chunksize;
	my $startchunk = do { use integer; $off / $chunksize };
	my $endchunk = do { use integer; ($off + $len - 1) / $chunksize };
	my $coff = $off % $chunksize;

	my $chunk = $self->getchunk($startchunk);
	my $chunklen = length($$chunk);
	return \'' unless $coff < $chunklen;
	return $chunk if $coff == 0 && $len == $chunklen;
	my $res = substr($$chunk, $coff, $len);

	for(my $i = $startchunk + 1; $i <= $endchunk; $i++) {
		last if length($res) == $len;
		$chunk = $self->getchunk($i);
		$chunklen = length($$chunk);
		my $appendlen = ($off + $len) % $chunksize;
		if($i == $endchunk && $appendlen != $chunklen) {
			$res .= substr($$chunk, 0, $appendlen);
		} else {
			$res .= $$chunk;
		}
		last if $chunklen < $chunksize;
	}

	return \$res;
}

sub read {
	my $len = shift;
	my $off = $self->off;
	unless(defined $len) {
		# just read to the end of the current chunk
		my $chunksize = $self->chunksize;
		$len = $chunksize - ($off % $chunksize);
	}
	my $res = $self->pread($off, $len);
	$self->off($off + length($$res));
	return $res;
}

sub seek {
	$self->off(shift);
	return;
}

sub getchunk {
	my $chunknum = shift;

	return $self->curchunkbuf if $self->curchunknum == $chunknum;

	my $chunksize = $self->chunksize;
	my $hashsize = $self->hashsize;

	my $digests = $self->digests;
	unless($chunknum < length($$digests) / $hashsize) {
		$self->curchunknum($chunknum);
		$self->curchunkbuf(\'');
		return \'';
	}

	$self->curchunknum_reset;
	$self->curchunkbuf_reset;

	my $digest = substr($$digests, $chunknum * $hashsize, $hashsize);
	my $chunkbuf = $self->pool->retrieve($digest);

	die "missing hash in pool\n"
		unless defined $chunkbuf;

	$self->curchunkbuf($chunkbuf);
	$self->curchunknum($chunknum);

	return $chunkbuf;
}

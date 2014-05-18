=encoding utf8

=head1 NAME

Fruitbak::Pool::Read - read file contents from the pooling system

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

package Fruitbak::Pool::Read;

use Class::Clarity -self;

use Digest::SHA qw(sha512);

field fbak => sub { $self->pool->fbak };
field pool;
field off => 0;
field digests;
field curchunkbuf;
field curchunknum => -1;
field chunksize => sub { $self->pool->chunksize };
field hashalgo => sub { $self->pool->hashalgo };
field hashsize => sub { $self->pool->hashsize };

sub new() {
	my $self = super;
	my $digests = $self->digests;
	my $hashsize = $self->hashsize;
	confess "invalid digests length"
		if length($digests) % $hashsize;
	return $self;
}

sub pread {
	my ($off, $len) = @_;
	my $chunksize = $self->chunksize;
	my $startchunk = do { use integer; $off / $chunksize };
	my $endchunk = do { use integer; ($off + $len - 1) / $chunksize };

	$self->getchunk($startchunk);
	my $coff = $off % $chunksize;
	return '' unless $coff < length($self->{curchunkbuf});
	my $res = substr($self->{curchunkbuf}, $coff, $len);

	for(my $i = $startchunk + 1; $i <= $endchunk; $i++) {
		last if length($res) == $len;
		$self->getchunk($i);
		if($i == $endchunk) {
			$res .= substr($self->{curchunkbuf}, 0, ($off + $len) % $chunksize);
		} else {
			$res .= $self->{curchunkbuf};
		}
		last if length($self->{curchunkbuf}) < $chunksize;
	}

	return $res;
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
	$self->off($off + length($res));
	return $res;
}

sub seek {
	$self->off(shift);
	return;
}

sub getchunk {
	my $chunk = shift;

	return if $self->curchunknum == $chunk;

	my $chunksize = $self->chunksize;
	my $hashsize = $self->hashsize;

	my $digests = $self->digests;
	return '' unless $chunk < length($digests) / $hashsize;

	$self->curchunknum_reset;
	$self->curchunkbuf_reset;

	my $digest = substr($digests, $chunk * $hashsize, $hashsize);

	my $pool = $self->pool;
	my $relsource = $pool->digest2path($digest);
	my $pooldir = $pool->pooldir;
	my $source = "$pooldir/$relsource";

	$self->curchunknum(-1);
	$self->curchunkbuf('');

	my $fh = new IO::File($source, '<:raw')
		or die "open($source): $!\n";
	for(;;) {
		my $r = $fh->read($self->{curchunkbuf}, $chunksize * 2, length($self->{curchunkbuf}));
		die "read($source): $!\n" unless defined $r;
		last unless $r;
	}
	$fh->close;
	undef $fh;

#	$self->hashalgo->($self->{curchunkbuf}) eq $digest
#		or die "Checksum mismatch for $source\n";
	$self->curchunknum($chunk);
}

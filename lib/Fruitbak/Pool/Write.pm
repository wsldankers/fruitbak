=encoding utf8

=head1 NAME

Fruitbak::Pool::Write - write file contents to disk

=head1 DESCRIPTION

This library defines a Fruitbak::Pool::Write class for writing
files to disk that are candidates for pooling. One instance
of this class is used to write each file. The following steps
are executed:

=over

=item 1) The incoming data is chunked into pieces of 2MiB each.

=item 2) Each chunk is hashed and written into the pool.

=item 3) A list of digests is kept in memory for later retrieval by the
caller.

=back

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

package Fruitbak::Pool::Write;

use Class::Clarity -self;

field fbak => sub { $self->pool->fbak };
field pool;
field buf => sub { my $x = ''; return \$x };
field digests => sub { my $x = ''; return \$x };
field hashsets => [];
field hashalgo => sub { $self->pool->hashalgo };
field hashsize => sub { $self->pool->hashsize };
field chunksize => sub { $self->pool->chunksize };

sub putchunk {
	my $chunk = shift;
	my $digests = $self->digests;
	my $hash = $self->hashalgo->($$chunk);
	my $hashsize = length($hash);
	my $digestslen = length($$digests);
	$self->pool->store($hash, $chunk)
		unless grep { $_->exists($hash) } @{$self->hashsets};
	$$digests .= $hash;
	return;
}

sub write {
	my $data = shift;
	my $datalen = length($$data);
	my $buf = $self->buf;
	my $buflen = length($$buf);
	my $chunksize = $self->chunksize;

	if($datalen == 0 && $datalen == $chunksize) {
		$self->putchunk($data);
		return;
	}

	$$buf .= $$data;
	$buflen += $datalen;

	if($buflen == $chunksize) {
		$self->putchunk($buf);
		$$buf = '';
		return;
	}

	while($buflen > $chunksize) {
		my $chunk = substr($$buf, 0, $chunksize, '');
		$self->putchunk(\$chunk);
		$buflen -= $chunksize;
	}

	return;
}

# Finish writing. Returns a 2 element list:
#
#	(digestString, outputFileLength)
#
sub close {
	my $hashsize = $self->hashsize;
	my $chunksize = $self->chunksize;

	my $buf = $self->buf;
	my $len = length($$buf);
	my $digests = $self->digests;
	my $size = length($$digests) / $hashsize * $chunksize + $len;
	$self->putchunk($buf)
		if $len;

	$self->buf_reset;

	return $$digests, $size;
}

# Abort a pool write
sub abort {
	$self->buf_reset;
}

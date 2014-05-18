=encoding utf8

=head1 NAME

Fruitbak::Pool::Write - write file contents to disk

=head1 DESCRIPTION

This library defines a Fruitbak::Pool::Write class for writing
files to disk that are candidates for pooling.  One instance
of this class is used to write each file.  The following steps
are executed:

=over

=item 1) The incoming data is chunked into pieces of 2MiB each.

=item 2) Each chunk is hashed and written into the pool.

=item 3) A list of hashes is kept in memory for later retrieval by the
caller.

=back

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

package Fruitbak::Pool::Write;

use Digest::SHA qw(sha512);
use File::Path qw(make_path);

use Class::Clarity -self;

field fbak => sub { $self->pool->fbak };
field pool;
field buf => '';
field hashes => '';
field chunksize => sub { $self->pool->chunksize };
field hashalgo => sub { $self->pool->hashalgo };
field hashsize => sub { $self->pool->hashsize };

sub new() {
	my $self = super;
	$self->buf;
	$self->hashes;
	return $self;
}

sub write {
	$self->{buf} .= $_[0];

	my $chunksize = $self->chunksize;

	while(length($self->{buf}) >= $chunksize) {
		my $chunk = substr($self->{buf}, 0, $chunksize, '');
		$self->{hashes} .= $self->savechunk($chunk);
	}
}

# Finish writing. Returns a 2 element list:
#
#	(digestString, outputFileLength)
#
sub close {
	my $hashsize = $self->hashsize;
	my $chunksize = $self->chunksize;
	my $size = length($self->{hashes}) / $hashsize * $chunksize + length($self->{buf});

	if(length($self->{buf})) {
		my $chunk = $self->{buf};
		$self->{hashes} .= $self->savechunk($chunk);
	}
	delete $self->{buf};

	return $self->{hashes}, $size;
}

# Abort a pool write
sub abort {
	$self->buf_reset;
}

sub savechunk {
	my $chunk = shift;

	my $hashalgo = $self->hashalgo;

	my $digest = $hashalgo->($chunk);

	my $pool = $self->pool;

	my $pooldir = $pool->pooldir;
	my $dest = $pooldir.'/'.$pool->digest2path($digest);

	return $digest if -e $dest;

	my $partial = "$pooldir/new-$$";
	unlink($partial) or $!{ENOENT}
		or die "unlink($partial): $!\n";
	my $fh = new IO::File("$partial", '>:raw')
		or die "open($partial): $!\n";
	my $off = 0;
	while($off < length($chunk)) {
		my $r = $fh->syswrite($chunk, length($chunk) - $off, $off)
			or die "write($partial): $!\n";
		$off += $r;
	}
	$fh->flush
		or die "write($partial): $!\n";
#	$fh->sync
#		or die "fsync($partial): $!\n";
	$fh->close
		or die "write($partial): $!\n";
	undef $fh;

	unless(rename($partial, $dest)) {
		die "rename($partial, $dest): $!\n" unless $!{ENOENT};
		my $dir = $dest;
		$dir =~ s{/[^/]*$}{};
		make_path($dir, {error => \my $err});
		die map { my ($file, $message) = %$_; $file ? "mkdir($file): $message\n" : "$message\n" } @$err
			if @$err;
		rename($partial, $dest)
			or die "rename($partial, $dest): $!\n";
	}

	return $digest;
}

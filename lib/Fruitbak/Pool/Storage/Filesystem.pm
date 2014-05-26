=encoding utf8

=head1 NAME

Fruitbak::Pool::Storage::Filesystem - store and retrieve chunks in files

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

package Fruitbak::Pool::Storage::Filesystem;

use IO::File;
use MIME::Base64;
use File::Path qw(make_path);

use Fruitbak::Util;

use Fruitbak::Pool::Storage -self;

field dir => sub {
	my $globalcfg = $self->fbak->cfg;
	return normalize_and_check_directory($self->cfg->{dir} // $globalcfg->rootdir . '/pool');
};

field do_fsync => sub { $self->cfg->{fsync} };

field chunksize => sub { $self->pool->chunksize };

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

sub store {
	my ($digest, $chunk) = @_;

	my $dir = $self->dir;
	my $reldest = $self->digest2path($digest);
	my $dest = "$dir/$reldest";

	return if -e $dest;

	my $partial = "$dir/new-$$";
	unlink($partial) or $!{ENOENT}
		or die "unlink($partial): $!\n";
	my $fh = new IO::File("$partial", '>:raw')
		or die "open($partial): $!\n";
	my $off = 0;
	my $chunklen = length($$chunk);
	while($off < $chunklen) {
		my $r = $fh->syswrite($$chunk, $chunklen - $off, $off)
			or die "write($partial): $!\n";
		$off += $r;
	}
	$fh->flush
		or die "write($partial): $!\n";
	if($self->do_fsync) {
		$fh->sync
			or die "fsync($partial): $!\n";
	}
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

	return;
}

sub retrieve {
	my $digest = shift;
	my $relsource = $self->digest2path($digest);
	my $dir = $self->dir;
	my $source = "$dir/$relsource";
	my $fh = new IO::File($source, '<:raw');
	unless($fh) {
		return undef if $!{ENOENT};
		die "open($source): $!\n";
	}
	my $buf = '';
	my $chunksize = $self->chunksize;
	for(;;) {
		my $r = $fh->sysread($buf, $chunksize * 2, length($buf));
		die "read($source): $!\n" unless defined $r;
		last unless $r;
	}
	$fh->close;
	undef $fh;
	return \$buf;
}

sub has {
	my $digest = shift;
	my $dir = $self->dir;
	my $reldest = $self->digest2path($digest);
	my $dest = "$dir/$reldest";

	return -e $dest;
}

sub remove {
	my $digest = shift;
	my $dir = $self->dir;
	my $reldest = $self->digest2path($digest);
	my $dest = "$dir/$reldest";

	unlink($dest) or $!{ENOENT}
		or die "unlink($dest): $!\n";
}

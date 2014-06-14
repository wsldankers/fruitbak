=encoding utf8

=head1 NAME

Fruitbak::Transfer::Rsync - Fruitbak module to backup files on the local system

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

package Fruitbak::Transfer::Local;

use Class::Clarity -self;

use autodie;

use IO::Handle;
use Fcntl qw(:mode);
use File::Hashset;
use File::Find;

use Fruitbak::Util;

weakfield fbak => sub { $self->host->fbak };
weakfield pool => sub { $self->fbak->pool };
weakfield host => sub { $self->backup->host };
weakfield backup => sub { $self->share->backup };
weakfield share;
field cfg;
field refbackup => sub { $self->share->refbackup };
field refshare => sub { $self->share->refshare };
field refhashes => sub {
	my $refbackup = $self->refbackup;
	return undef unless defined $refbackup;
	return $refbackup->hashes;
};
field hashsize => sub { $self->pool->hashsize };
field chunksize => sub { $self->pool->chunksize };
field curfile;
field curfile_attrs;
field curfile_blocksize;
field inodes => {};
field path => sub { normalize_path($self->share->path) };
field pathre => sub {
	my $path = quotemeta($self->path);
	return qr{^$path/};
};

sub reffile {
	my $name = shift;
	if(my $refshare = $self->refshare) {
		return $refshare->get_entry($name);
	}

	return undef;
}

sub wanted {
	my $path = $self->path;
	my $pathre = $self->pathre;
	my $is_hardlink;
	my @st = lstat($_);
	my $relpath = $_;
	unless($relpath =~ s{$pathre}{}a) {
		if(-d _) {
			$relpath = '.';
		} else {
			$relpath = $path =~ s{^.*/}{}ar;
		}
	}
	my $dentry = new Fruitbak::Dentry(
		name => $relpath,
		mode => $st[2],
		size => $st[7],
		mtime => $st[9],
		uid => $st[4],
		gid => $st[5],
	);
	if(!-d _ && $st[3] > 1) {
		# potentially a hardlink
		my $inodes = $self->inodes;
		my $inode = "$st[0]$;$st[1]";
		if(exists $inodes->{$inode}) {
			$dentry->hardlink($inodes->{$inode});
			$is_hardlink = 1;
		} else {
			$inodes->{$inode} = $relpath;
		}
	}
	if(!$is_hardlink) {
		if(-f _) {
			if(-s _) {
				my $reffile = $self->reffile($relpath);
				if($self->backup->type ne 'full'
						&& $reffile
						&& $reffile->is_file
						&& $reffile->size == $dentry->size
						&& $reffile->uid == $dentry->uid
						&& $reffile->gid == $dentry->gid
						&& $reffile->mode == $dentry->mode
						&& $reffile->mtime == $dentry->mtime) {
					$dentry->digests($reffile->digests);
				} else {
					my @refhashes;
					push @refhashes, new File::Hashset($reffile->digests, $self->hashsize)
						if $reffile && $reffile->is_file;
					if(my $refhashes = $self->refhashes) {
						push @refhashes, $refhashes;
					}
					my $writer = new Fruitbak::Pool::Write(pool => $self->pool, refhashes => \@refhashes);
					my $chunksize = $self->chunksize;
					local $@;
					eval {
						my $fh = new IO::File($_, '<')
							or die "open($_): $!\n";
						for(;;) {
							my $r = sysread($fh, my $buf, $chunksize);
							die "read($_): $!\n" unless defined $r;
							last if !$r;
							$writer->write(\$buf);
							last if $r < $chunksize;
						}
						$fh->close;
					};
					warn $@ if $@;
					my ($digests, $size) = $writer->close;
					$dentry->size($size) if $digests ne '';
					$dentry->digests($digests);
				}
			}
		} elsif(-l _) {
			my $symlink = readlink($_);
			if(defined $symlink) {
				$dentry->symlink($symlink);
			} else {
				warn "unable to read symlink '$_': $!\n";
				return;
			}
		} elsif(-b _ || -c _) {
			$dentry->rdev($st[6]);
		}
	}
	$self->share->add_entry($dentry);
}

sub recv_files {
	find({ wanted => sub { $self->wanted(@_) }, no_chdir => 1, follow => 0}, $self->share->path);
}

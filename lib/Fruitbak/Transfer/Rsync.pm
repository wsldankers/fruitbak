=encoding utf8

=head1 NAME

Fruitbak::Transfer::Rsync - transfer files using rsync

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

package Fruitbak::Transfer::Rsync;

use Class::Clarity -self;

use autodie;

use File::RsyncP::Digest;
use IPC::Open2;
use IO::Handle;
use POSIX qw(:sys_wait_h);
use Fcntl qw(:mode);
use File::Hashset;

use Fruitbak::Transfer::Rsync::RPC;

weakfield fbak => sub { $self->host->fbak };
weakfield pool => sub { $self->fbak->pool };
weakfield host => sub { $self->backup->host };
weakfield backup => sub { $self->share->backup };
weakfield share;
field cfg;
field command => sub {
	$self->cfg->{command} //
		q{exec ssh ${port+-p "$port"} ${user+-l "$user"} $host exec rsync "$@"}
};
field refbackup => sub { $self->share->refbackup };
field refshare => sub { $self->share->refshare };
field refhashes => sub {
	my $refbackup = $self->refbackup;
	return undef unless defined $refbackup;
	return $refbackup->hashes;
};
field hashsize => sub { $self->pool->hashsize };
field curfile;
field curfile_attrs;
field curfile_blocksize;
field deltafile;
field csumfile;
field protocol_version;
field checksumSeed;
field digest => sub { new File::RsyncP::Digest($self->protocol_version) };
field csumDigest => sub { new File::RsyncP::Digest($self->protocol_version) };
field rpc;

sub attrs2dentry() {
	my $attrs = shift;
	my $dentry = new Fruitbak::Dentry(
		name => $attrs->{name},
		mode => $attrs->{mode},
		size => $attrs->{size},
		mtime => $attrs->{mtime},
		uid => $attrs->{uid},
		gid => $attrs->{gid},
		@_
	);
	if(exists $attrs->{hlink} && !$attrs->{hlink_self}) {
		$dentry->hardlink($attrs->{hlink})
	} else {
		if($dentry->is_symlink) {
			$dentry->symlink($attrs->{link})
		} elsif($dentry->is_device) {
			$dentry->rdev_major($attrs->{rdev_major});
			$dentry->rdev_minor($attrs->{rdev_minor});
		}
	}
	return $dentry;
}

sub dentry2attrs() {
	my $dentry = shift;
	return undef unless defined $dentry;
	my %attrs = (
		name => $dentry->name,
		mode => $dentry->mode,
		size => $dentry->size,
		mtime => $dentry->mtime,
		uid => $dentry->uid,
		gid => $dentry->gid,
	);
	if($dentry->is_device) {
		$attrs{rdev_major} = $dentry->rdev_major;
		$attrs{rdev_minor} = $dentry->rdev_minor;
	} elsif($dentry->is_symlink) {
		$attrs{link} = $dentry->symlink;
	}
	return \%attrs;
}

sub setup_reffile {
	my ($attrs, $which) = @_;
	my $reset = $which.'_reset';
	$self->$reset;
	if(my $refshare = $self->refshare) {
		if(my $dentry = $refshare->get_entry($attrs->{name})) {
			# File::RsyncP doesn't care if the entry exists but is another
			# type. It will stubbornly ask for the data anyway. Simulate
			# an empty file in that case.
			my $digests = $dentry->is_file ? $dentry->digests : '';
			my $poolreader = $self->pool->reader(digests => $digests);
			$self->$which($poolreader);
		}
	}
}

sub attribGet {
	return undef if $self->backup->full;
	my $attrs = shift;
	my $ref = $self->refshare;
	return unless $ref;
	my $dentry = $ref->get_entry($attrs->{name});
	return undef unless $dentry && $dentry->is_file;
	my $a = dentry2attrs($dentry);
	$a->{name} = $attrs->{name};
	$a->{hlink_self} = $attrs->{hlink_self}
		if exists $attrs->{hlink_self};
	return $a;
}

sub fileDeltaRxStart {
	my ($attrs, $numblocks, $blocksize, $lastblocksize) = @_;
	$self->setup_reffile($attrs, 'deltafile');
	my $writer = $self->pool->writer;
	if(my $refhashes = $self->refhashes) {
		my @hashsets = ($refhashes);
		unshift @hashsets, new File::Hashset(${$self->deltafile->digests}, $self->hashsize)
			if $self->deltafile_isset;
		$writer->hashsets(\@hashsets);
	}
	$self->curfile($writer);
	$self->curfile_attrs($attrs);
	$self->curfile_blocksize($blocksize);
}

sub fileDeltaRxNext {
	my $blocknum = shift;
	my $curfile = $self->curfile;
	if(defined $blocknum) {
		my $deltafile = $self->deltafile;
		my $blocksize = $self->curfile_blocksize;
		my $data = $deltafile->pread($blocknum * $blocksize, $blocksize);
		$curfile->write($data);
	} else {
		$curfile->write(\($_[0]));
	}
	return 0;
}

sub fileDeltaRxDone {
	my ($hashes, $size) = $self->curfile->close;
	my $dentry = attrs2dentry($self->curfile_attrs, digests => $hashes, size => $size);
	$self->share->add_entry($dentry);
	$self->curfile_reset;
	return undef;
}

sub csumStart {
    my ($attrs, $needMD4, $blockSize, $phase) = @_;

	$self->csumEnd if $self->csumfile_isset;

	$self->setup_reffile($attrs, 'csumfile');
	confess("csumStart called without an existing file\n")
		unless $self->csumfile_isset;

	$self->csumDigest_reset;
	$self->csumDigest->add(pack('V', $self->checksumSeed))
		if $needMD4;

	return $blockSize;
}

sub csumGet {
	confess("csumGet called without a csum_file\n")
		unless $self->csumfile_isset;

	my ($num, $csumLen, $blockSize) = @_;

	$num ||= 100;
	$csumLen ||= 16;

	my $data = $self->csumfile->read($blockSize * $num);

	$self->csumDigest->add($$data)
		if $self->csumDigest_isset;

	return $self->digest->blockDigest($$data, $blockSize, $csumLen, $self->checksumSeed);
}

sub csumEnd {
	confess("csumEnd called without a csum_file\n")
		unless $self->csumfile_isset;
	my $csumfile = $self->csumfile;
	$self->csumfile_reset;
	if($self->csumDigest_isset) {
		# read the rest of the file for the MD4 digest
		my $csumDigest = $self->csumDigest;
		for(;;) {
			my $data = $csumfile->read;
			last if $data eq '';
			$csumDigest->add($$data);
		}
		return $csumDigest->digest;
	}
	return undef;
}

sub attribSet {
	my ($attrs, $placeHolder) = @_;

	my $dentry = attrs2dentry($attrs);
	# if this is an existing regular file:
	if($dentry->is_file && !$dentry->is_hardlink) {
		if(my $refshare = $self->refshare) {
			if(my $ref = $refshare->get_entry($attrs->{name})) {
				unless($ref->is_file) {
					# If the type of the file changes, File::RsyncP may
					# behave strangely. Handle that.
					return undef;
				}
				$dentry->size($ref->size);
				$dentry->digests($ref->digests);
			}
		}
	}
	$self->share->add_entry($dentry);
	return undef;
}

sub reply_rpc {
	my $in = $self->rpc;
	print $in pack('L', length($_[0])), $_[0];
	$in->flush or die "Unable to write to filehandle: $!\n";
}

sub recv_files {
	local $SIG{PIPE} = 'IGNORE';
	my $path = $self->share->path;
die "REMOVE BEFORE FLIGHT" if $path eq '/';
	$path =~ s{(?:/+\.?)?$}{/.}a;
	my $pid = open2(my $out, my $in,
		'fruitbak-rsyncp-recv',
		$self->command,
		$path,
		qw(
			--numeric-ids
			--perms
			--owner
			--group
			--devices
			--links
			--recursive
			--hard-links
			--times
			--specials
			--block-size=131072
			--whole-file
		));
	local $@;
	eval {
		$in->binmode;
		$out->binmode;
		$self->rpc($in);
		for(;;) {
			my ($len, $cmd) = unpack('LC', saferead($out, 5));
			die "internal protocol error: unknown RPC opcode $cmd\n"
				if $cmd > RSYNC_RPC_max;
			my $data = saferead($out, $len) if $len;
			if($cmd == RSYNC_RPC_finish) {
				last;
			} elsif($cmd == RSYNC_RPC_protocol_version) {
				$self->protocol_version(unpack('L', $data));
			} elsif($cmd == RSYNC_RPC_checksumSeed) {
				$self->checksumSeed(unpack('L', $data));
			} elsif($cmd == RSYNC_RPC_attribGet) {
				my $attrs = $self->attribGet(parse_attrs($data));
				$self->reply_rpc(serialize_attrs($attrs) // '');
			} elsif($cmd == RSYNC_RPC_fileDeltaRxStart) {
				my ($numblocks, $blocksize, $lastblocksize) = unpack('QLL', $data);
				my $attrs = parse_attrs(substr($data, 16));
				$self->fileDeltaRxStart($attrs, $numblocks, $blocksize, $lastblocksize);
			} elsif($cmd == RSYNC_RPC_fileDeltaRxNext_blocknum) {
				$self->fileDeltaRxNext(unpack('Q', $data), undef);
			} elsif($cmd == RSYNC_RPC_fileDeltaRxNext_data) {
				$self->fileDeltaRxNext(undef, $data);
			} elsif($cmd == RSYNC_RPC_fileDeltaRxDone) {
				$self->fileDeltaRxDone;
			} elsif($cmd == RSYNC_RPC_csumStart) {
				my ($needMD4, $blockSize, $phase) = unpack('CLC', $data);
				my $attrs = parse_attrs(substr($data, 6));
				$self->csumStart($attrs, $needMD4, $blockSize, $phase);
			} elsif($cmd == RSYNC_RPC_csumGet) {
				my ($num, $csumLen, $blockSize) = unpack('QLL', $data);
				$self->reply_rpc($self->csumGet($num, $csumLen, $blockSize));
			} elsif($cmd == RSYNC_RPC_csumEnd_digest) {
				$self->reply_rpc($self->csumEnd);
			} elsif($cmd == RSYNC_RPC_csumEnd) {
				$self->csumEnd;
			} elsif($cmd == RSYNC_RPC_attribSet) {
				$self->attribSet(parse_attrs($data));
			} else {
				die "internal protocol error: unknown RPC opcode $cmd\n";
			}
		}
	};
	my $err = $@;
	my $signaled;
	my $reaped;
	eval {
		my $handler = sub {
			if(eval { waitpid($pid, WNOHANG) }) {
				$reaped = 1;
				die "child exited\n";
			}
		};
		local $SIG{CHLD} = $handler;
		$handler->();
		sleep(2);
		$signaled = 1;
		kill TERM => $pid;
		sleep(2);
		kill KILL => $pid;
	};
	waitpid($pid, 0) unless $reaped;
	die $err if $err;
	if(WIFEXITED($?)) {
		my $status = WEXITSTATUS($?);
		die sprintf("sub-process exited with status %d\n", $status)
			if $status;
	} elsif(WIFSIGNALED($?)) {
		my $sig = WTERMSIG($?);
		my $msg = sprintf("sub-process killed with signal %d%s\n", $sig & 127, ($sig & 128) ? ' (core dumped)' : '');
		if($signaled) {
			warn $msg;
		} else {
			die $msg;
		}
	}
	return;
}

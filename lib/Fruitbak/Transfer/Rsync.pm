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

use autodie;

use File::RsyncP::Digest;
use Fruitbak::Transfer::Rsync::RPC;
use IPC::Open2;
use IO::Handle;
use POSIX qw(:sys_wait_h);
use Fcntl qw(:mode);

use Class::Clarity -self;

field fbak => sub { $self->host->fbak };
field pool => sub { $self->fbak->pool };
field host => sub { $self->backup->host };
field backup => sub { $self->share->backup };
field share;
field refshare => sub { $self->share->refshare };
field curfile;
field curfile_attrs;
field curfile_blocksize;
field reffile;
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
	my $attrs = shift;
	if(my $refshare = $self->refshare) {
		if(my $dentry = $refshare->get_entry($attrs->{name})) {
			if($dentry->is_file) {
				my $poolreader = $self->pool->reader(digests => $dentry->digests);
				$self->reffile($poolreader);
			}
		}
	}
}

sub attribGet {
	my $attrs = shift;
	my $ref = $self->refshare;
	return unless $ref;
	return dentry2attrs($ref->get_entry($attrs->{name}));
}

sub fileDeltaRxStart {
	my ($attrs, $numblocks, $blocksize, $lastblocksize) = @_;
	my $pool = $self->pool;
	$self->curfile($pool->writer);
	$self->curfile_attrs($attrs);
	$self->curfile_blocksize($blocksize);
	$self->setup_reffile($attrs);
}

sub fileDeltaRxNext {
	my $blocknum = shift;
	my $curfile = $self->curfile;
	if(defined $blocknum) {
		return unless defined $blocknum;
		my $reffile = $self->reffile;
		die "No reffile but \$data undef? blocknum=$blocknum\n"
			unless defined $reffile;
		my $blocksize = $self->curfile_blocksize;
		my $data = $reffile->pread($blocknum * $blocksize, $blocksize);
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

	$self->csumEnd if $self->reffile_isset;

	my $refshare = $self->refshare or return;
	my $dentry = $refshare->get_entry($attrs->{name}) or return;
	return unless $dentry->is_file;

	$self->setup_reffile($attrs);

	$self->csumDigest_reset;
	$self->csumDigest->add(pack('V', $self->checksumSeed))
		if $needMD4;

	return $blockSize;
}

sub csumGet {
	return unless $self->reffile_isset;

	my ($num, $csumLen, $blockSize) = @_;

	$num ||= 100;
	$csumLen ||= 16;

	my $data = $self->reffile->read($blockSize * $num);

	$self->csumDigest->add($$data)
		if $self->csumDigest_isset;

	return $self->digest->blockDigest($$data, $blockSize, $csumLen, $self->checksumSeed);
}

sub csumEnd {
	return unless $self->reffile_isset;
	my $reffile = $self->reffile;
	$self->reffile_reset;
	if($self->csumDigest_isset) {
		# read the rest of the file for the MD4 digest
		my $csumDigest = $self->csumDigest;
		for(;;) {
			my $data = $reffile->read;
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
				$dentry->size($ref->size);
				$dentry->digests($ref->digests);
			}
		}
	}
	$self->share->add_entry($dentry);
	return undef;
}

sub finish {
	$self->share->finish;
}

sub reply_rpc {
	my $in = $self->rpc;
	print $in pack('L', length($_[0])), $_[0];
	$in->flush or die "Unable to write to filehandle: $!\n";
}

sub recv_files {
	local $SIG{PIPE} = 'IGNORE';
	my $name = $self->share->name;
	$name =~ s{(?:/+\.?)?$}{/.}a;
	my $pid = open2(my $out, my $in,
		'fruitbak-rsyncp-recv',
		$name,
		qw(rsync
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
		));
	local $@;
	eval {
		$in->binmode;
		$out->binmode;
		$self->rpc($in);
		for(;;) {
			my ($len, $cmd) = unpack('LC', saferead($out, 5));
			my $data = saferead($out, $len) if $len;
			if($cmd == RSYNC_RPC_finish) {
				$self->finish;
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
				die "Internal protocol error: unknown RPC opcode $cmd\n";
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

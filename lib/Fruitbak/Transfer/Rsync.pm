=encoding utf8

=head1 NAME

Fruitbak::Transfer::Rsync - Fruitbak class to transfer files using rsync

=head1 SYNOPSIS

 my $rsync = new Fruitbak::Transfer::Rsync(share => $share, cfg => \%cfg);
 $rsync->recv_files;

=head1 DESCRIPTION

This class uses the rsync protocol to transfer files from clients and store
them in a Fruitbak::Share::Write object. It calls $share->add_entry for
every file found.

The rsync protocol is not implemented by this class. It interfaces with a
separate program (fruitbak-rsyncp-recv) which in turn uses File::RsyncP to
do the heavy lifting. To communicate with fruitbak-rsyncp-recv we use a
simple binary RPC protocol.

The reason for this split is two-fold: first, File::RsyncP expects to be
able to use fork(), which wreaks havoc on any open file, socket and
database handles. Second, it allows for parallelization of the rsync
process and Fruitbak's own I/O.

=cut

package Fruitbak::Transfer::Rsync;

use Class::Clarity -self;

use File::RsyncP::Digest;
use IPC::Open2;
use IO::Handle;
use POSIX qw(:sys_wait_h);
use Fcntl qw(:mode);
use File::Hashset;

use Fruitbak::Transfer::Rsync::RPC;

=head1 FIELDS

Fruitbak makes heavy use of Class::Clarity (the base class of this class).
One of the features of Class::Clarity is ‘fields’: elements of the object
hash with eponymous getters and setters. These fields can optionally have
an initializer: a function that is called when no value is yet assigned to
the hash element. For more information, see L<Class::Clarity>.

=over

=item field fbak

The Fruitbak object that is the root ancestor of this transfer method. Do
not set.

=cut

weakfield fbak => sub { $self->host->fbak };

=item pool

The pool object that will be used to store and retrieve file data. Do not
set.

=cut

weakfield pool => sub { $self->fbak->pool };

=item host

The host to which this newly created share belongs. Do not set.

=cut

weakfield host => sub { $self->backup->host };

=item backup

The backup to which this newly created share belongs. Do not set.

=cut

weakfield backup => sub { $self->share->backup };

=item share

The share that will receive the remote files.

=cut

weakfield share;

=item cfg

The configuration for this transfer method. Should be a hash with keys
as described in the configuration manual. Any host-wide configuration
variables should be pre-merged, that is, Fruitbak::Transfer::Rsync will
only check this one hash for configuration parameters.

=cut

field cfg;

=item command

The command, as configured, to start the rsync server counterpart on the
remote machine. This command is passed to the helper tool.

=cut

field command => sub {
	$self->cfg->{command} //
		q{exec ssh ${port+-p "$port"} ${user+-l "$user"} -- "$host" exec rsync "$@"}
};

=item refbackup

The Fruitbak::Backup::Read object to which the refshare belongs.

=cut

field refbackup => sub { $self->share->refbackup };

=item refshare

A Fruitbak::Share::Read object that serves as a reference for the currently
running backup. Do not set.

=cut

field refshare => sub { $self->share->refshare };

=item refhashes

A File::Hashset object that may be queried to see if hashes are already in
the pool. Do not set.

=cut

field refhashes => sub {
	my $refbackup = $self->refbackup;
	return undef unless defined $refbackup;
	return $refbackup->hashes;
};

=item hashsize

The length of the hashes as stored in the pool. Do not set.

=cut

field hashsize => sub { $self->pool->hashsize };

=item wholefile

Prevent rsync from doing delta transfers.

=cut

field wholefile => sub { $self->cfg->{wholefile} };

=item curfile

When receiving files, this is a handle to the Fruitbak::Pool::Write object
that will process the file contents for the file that is currently being
received. For internal use only.

=cut

field curfile;

=item curfile_attrs

The attributes (as received from File::RsyncP) for the file currently being
processed. When done receiving the file they will be stored as its metadata.

=cut

field curfile_attrs;

=item curfile_blocksize

The rsync blocksize for the file that is currently being received. We keep
this information because we may need to feed it to File::RsyncP::Digest if
we're doing differential transmissions.

=cut

field curfile_blocksize;

=item deltafile

When doing differential transmissions of files, this is the
Fruitbak::Pool::Read object that we use as the basis for bits that rsync
has determined are unmodified and can be copied without transferring them
over the network.

This must be the same data that was used to generate checksums from, but
because rsync is parallellized to a large degree, csumfile (see below) may
already point to the next file that is being processed.

=cut

field deltafile;

=item csumfile

When doing differential transmissions of files, this is the
Fruitbak::Pool::Read object that we use when File::RsyncP requests
checksums for a file.

This must be the same data that will be used later on to fill in parts that
were determined to already be available, but because rsync is parallellized
to a large degree, deltafile (see above) may still point to a previous file
when we're already generating checksums for the next.

=cut

field csumfile;

=item protocol_version

The rsync protocol version, needed for File::RsyncP::Digest if
we're doing differential transmissions.

=cut

field protocol_version;

=item checksumSeed

The seed for rsync's checksum algorithm, needed for File::RsyncP::Digest if
we're doing differential transmissions.

=cut

field checksumSeed;

=item digest

The File::RsyncP::Digest object that we will use for generating block
digests when we're doing differential transmissions. Since it does not
(need to) keep state for this usage, we keep the same instance around for
the entire lifetime of this Fruitbak::Transfer::Rsync object.

=cut

field digest => sub { new File::RsyncP::Digest($self->protocol_version) };

=item csumDigest

The File::RsyncP::Digest object that we will use for generating whole-file
MD4 digests when we're doing differential transmissions. To this end it
needs to keep the MD4 state, so this field only has a value when we're
actually calculating MD4 digests for a file, and we destroy it when we're
done with it.

=cut

field csumDigest => sub { new File::RsyncP::Digest($self->protocol_version) };

=item rpc

The internal filehandle that we use to communicate with the File::RsyncP
wrapper.

=cut

field rpc;

=back

=head1 FUNCTIONS

=over

=item attrs2dentry($attrs, ...)

Converts an attributes hash (in the format that File::RsyncP uses) to a
Fruitbak::Dentry object. The first argument should be a File::RsyncP
attributes hash, any subsequent arguments will be passed as-is to the
Fruitbak::Dentry constructor.

=cut

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

=item dentry2attrs($dentry)

Converts a Fruitbak::Dentry object to an attributes hash in the format that
File::RsyncP uses.

=cut

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

=back

=head1 METHODS

=over

=item setup_reffile($which)

This is the method that is used to set up csumfile and deltafile (see
above). The which argument is a simple string that is either "csumfile" or
"deltafile". If the file does not exist, it unsets the requested field.

=cut

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

=item filter_options

Returns a list of File::RsyncP compatible --include/--exclude options based
on the generic host exclusions, the generic per-share exclusions and the
rsync-specific per-share exclusions.
Will die() if an unsupported filter type is requested.

=cut

field filter_options => sub {
	my @options;
	my $generic = $self->share->exclude;
	foreach(@$generic) {
		my $copy = $_;
		$copy =~ s/([][*])/\\$1/ga;
		push @options, "--exclude=/$copy";
	}
	my $filter = $self->cfg->{filter} // [];
	die "no rsync-specific filters supported yet\n"
		if @$filter;
#	push @options, grep { /^[+-] / } @$filter;
	return \@options;
};

=item extra_options

User-specified extra options that will be passed as-is to the remote rsync
process.

=cut

field extra_options => sub { $self->cfg->{extra_options} // [] };

=item attribGet($attrs)

This File::RsyncP callback retrieves file attributes from the reference
backup, given the attributes as they were detected on the remote filesystem
by rsync, so that File::RsyncP can compare them to see if the file needs
transferring.

=cut

sub attribGet {
	return undef if $self->backup->full;
	my $attrs = shift;
	my $ref = $self->refshare;
	return unless $ref;
	my $dentry = $ref->get_entry($attrs->{name});
	return undef unless $dentry && $dentry->is_file;
	my $a = dentry2attrs($dentry);
	return undef if $self->wholefile
		&& $a->{mtime} == $attrs->{mtime}
		&& $a->{size} == $attrs->{size}
		&& $a->{uid} == $attrs->{uid}
		&& $a->{gid} == $attrs->{gid}
		&& $a->{mode} == $attrs->{mode};
	$a->{name} = $attrs->{name};
	# rsync's model of hardlinks is a bit weird, kludge around that
	$a->{hlink_self} = $attrs->{hlink_self}
		if exists $attrs->{hlink_self};
	return $a;
}

=item fileDeltaRxStart($attrs, $numblocks, $blocksize, $lastblocksize)

Invocation of this File::RsyncP callback indicates that rsync wants to
transfer file data. We record the information we get through the arguments
and use the opportunity to set up the curfile and deltafile fields.

=cut

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

=item fileDeltaRxNext($blocknum, $data)

This File::RsyncP callback is called whenever rsync has new available data
or has detected that we can reuse some data from the previous version of
this file (deltafile).

=cut

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

=item fileDeltaRxDone($hashes, $size)

This File::RsyncP callback is called when rsync is finished transferring
the file data. This is where we store the Fruitbak digests and regular
metadata.

=cut

sub fileDeltaRxDone {
	my ($hashes, $size) = $self->curfile->close;
	my $dentry = attrs2dentry($self->curfile_attrs, digests => $hashes, size => $size);
	$self->share->add_entry($dentry);
	$self->curfile_reset;
	return undef;
}

=item csumStart($attrs, $needMD4, $blockSize, $phase)

This is the File::RsyncP callback that it calls when it has found a file on
the remote end that we have in our reference backup. File::RsyncP calling
this indicates that it will ask us for checksums of this file. We record
the information we get through the arguments and use the opportunity to set
up the csumfile field.

=cut

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

=item csumGet($num, $csumLen, $blockSize)

This is the File::RsyncP callback that it calls when it wants more
rsync-style checksums on the current file.

=cut

sub csumGet {
	confess("csumGet called without a csumfile\n")
		unless $self->csumfile_isset;

	my ($num, $csumLen, $blockSize) = @_;

	$num ||= 100;
	$csumLen ||= 16;

	my $data = $self->csumfile->read($blockSize * $num);

	$self->csumDigest->add($$data)
		if $self->csumDigest_isset;

	return $self->digest->blockDigest($$data, $blockSize, $csumLen, $self->checksumSeed);
}

=item csumEnd()

This File::RsyncP callback indicates that File::RsyncP has all the
checksums that it needs for this file. Cleans up csumfile and csumDigest;
returns the MD4 checksum if it was requested in csumStart.

=cut

sub csumEnd {
	confess("csumEnd called without a csum_file\n")
		unless $self->csumfile_isset;
	my $csumfile = $self->csumfile_reset;
	if($self->csumDigest_isset) {
		# read the rest of the file for the MD4 digest
		my $csumDigest = $self->csumDigest_reset;
		for(;;) {
			my $data = $csumfile->read;
			last if $data eq '';
			$csumDigest->add($$data);
		}
		return $csumDigest->digest;
	}
	return undef;
}

=item attribSet($attrs, $placeHolder)

This File::RsyncP callback is called when rsync has detected either a
non-file entry or an existing file that has similar enough attributes that
it doesn't warrant file content transfer. It is our cue to add the
entry to the backup. In the case of the existing file, we "copy" the
file contents by reusing the digest list.

=cut

sub attribSet {
	my ($attrs, $placeHolder) = @_;

	my $dentry = attrs2dentry($attrs);
	# if this is an existing regular file:
	if($dentry->is_file && !$dentry->is_hardlink) {
		if(my $refshare = $self->refshare) {
			if(my $ref = $refshare->get_entry($attrs->{name})) {
				unless($ref->is_file) {
					# If the type of the file changes, File::RsyncP
					# sometimes behaves strangely. Handle that case
					# by simply ignoring it.
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

=item reply_rpc($data)

Send RPC data to our File::RsyncP wrapper child process and check for any
error conditions.

=cut

sub reply_rpc {
	my $in = $self->rpc;
	my $buf = pack('L', length($_[0])) . $_[0];
	my $r = syswrite $in, $buf;
	die "write(): $!\n" unless defined $r;
	confess("short write") if $r < length($buf);
}

=item recv_files()

Implementation of the entry point for transfer methods. It forks off the
File::RsyncP wrapper child process and implements the RPC protocol.

=cut

sub recv_files {
	local $SIG{PIPE} = 'IGNORE';
	my $path = $self->share->path;
	$path =~ s{(?:/+\.?)?$}{/.}a;
	my @wholefile = qw(--whole-file)
		if $self->wholefile;
	my $pid = open2(my $out, my $in,
		'fruitbak-rsyncp-recv',
		$self->command,
		$path,
		@{$self->filter_options},
		@wholefile,
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
				$self->reply_rpc(serialize_attrs($attrs));
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
				my ($blockSize, $needMD4, $phase) = unpack('LCC', $data);
				my $attrs = parse_attrs(substr($data, 6));
				$self->csumStart($attrs, $needMD4, $blockSize, $phase);
			} elsif($cmd == RSYNC_RPC_csumGet) {
				my ($num, $blockSize, $csumLen) = unpack('QLC', $data);
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

=back

=head1 AUTHOR

Wessel Dankers <wsl@fruit.je>

=head1 COPYRIGHT

Copyright (c) 2014,2015 Wessel Dankers <wsl@fruit.je>

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

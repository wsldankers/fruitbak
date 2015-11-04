=encoding utf8

=head1 NAME

Fruitbak::Transfer::Rsync::IO - mediate between Fruitbak and File::RsyncP

=head1 SYNOPSIS

 my $lock = new File::Temp(EXLOCK => 0);

 my $fio = new Fruitbak::Transfer::Rsync::IO(
 	in => $in,
 	out => $out,
 	lockfh => $lock,
 	lockpid => $$,
 );

 my $rs = new File::RsyncP({fio => $fio});

=head1 DESCRIPTION

This class implements the File::RsyncP::FileIO by marshalling all calls
over RPC. Each time a method is invoked, an RPC call is sent out over the
"out" filehandle. If the call requires a response, it is read from the "in"
filehandle. On the other end should be a class like
Fruitbak::Transfer::Rsync that responds to the RPC calls.

File::RsyncP forks the process, causing multiple clones of an
Fruitbak::Transfer::Rsync::IO object to spring into existence. This is, in
fact, one of the main reasons to run this code in a seperate process with
an RPC tether.

One important consequence of forking for our RPC channel is that multiple
processes will be reading from and writing to the channel at the same time.
That means we have to maintain a lock. Because it is well-defined across
forks, flock()-based locking was selected for this job.

We assume that the input and output file handles are POSIX pipes. This
means writes of up to PIPE_BUF bytes are atomic. We can exploit that
feature to maximize parallellism by using both shared and exclusive locks.

Exclusive locks are needed when the write is non-atomic or the RPC call
requires a reply. When the write is atomic and no reply is required, we can
obtain a shared lock. This will exclude the processes that are sending
complicated calls while processes that send simple calls can send those in
parallel.

The wire protocol is described in more detail in
Fruitbak::Transfer::Rsync::RPC.

=head1 CONSTRUCTOR

Required arguments to the constructor are "in", "out" and "lockname". If
you happen to have an open file handle for the lockfile, you can pass it in
"lockfh" (but in that case you must also pass lockpid => $$).

=cut

package Fruitbak::Transfer::Rsync::IO;

use File::RsyncP::Digest;
use Fruitbak::Transfer::Rsync::RPC;
use POSIX qw(PIPE_BUF);
use Fcntl qw(:flock);
use Guard;

use Class::Clarity -self;

=head1 FIELDS

Fruitbak makes heavy use of Class::Clarity (the base class of this class).
One of the features of Class::Clarity is ‘fields’: elements of the object
hash with eponymous getters and setters. These fields can optionally have
an initializer: a function that is called when no value is yet assigned to
the hash element. For more information, see L<Class::Clarity>.

=over

=item needMD4

Set during csumStart, this fields indicates whether an MD4 checksum was
requested by File::RsyncP for this checksum run. Iff this field is set
(either false or true), a csum run is in progress.

=cut

field needMD4;

=item blockSize

Used by File::RsyncP to store the blockSize of the current transfer.

=cut

field blockSize;

=item preserve_hard_links

Used by File::RsyncP to store whether hardlinks will be preserved.

=cut

field preserve_hard_links;

=item lockfh

The filehandle of a file that will be used for locking. It must still exist
on the filesystem (in other words, it must not be deleted).
Should usually be an instance of File::Temp (with EXLOCK set to false).
This file should persist during the rsync transfer run, so keep it around
in a seperate variable while File::RsyncP is running. Do not change after
initialization.

If you pass a lockfh to the initializer, you must also pass lockpid (set to
$$).

=cut

field lockfh => sub {
	open my $lockfh, '+<', $self->lockname
		or die "can't open $lockname: $!\n";
	$self->lockpid($pid);
	return $lockfh;
};

=item lockname

The filename of the lockfile. This is a required argument to the
constructor. Do not change after initialization.

=cut

field lockname;

=item lockpid

The pid of the process that opened the current lockfh. Should usually
simply be $$. If the object is trying to obtain a lock but its process ID
does not match this value, it assumes that a fork has taken place and the
lock file needs to be reopened.

=cut

field lockpid => 0;

=item in

The file descriptor from which replies are read. Required argument to the
constructor.

=cut

field in;

=item out

The file descriptor to which RPC calls are dispatched. Required argument to
the constructor. Must be a pipe.

=cut

field out;

=back

=head1 METHODS

=over

=item version

The File::RsyncP API version that we implement. Hardcoded to 2.

=cut

use constant version => 2;

=item guardlock

Takes a lock on the lockfile and returns an object that, when discarded,
unlocks the lockfile again. If the registered process ID does not match the
current one, the lockfile is reopened.

=cut

sub guardlock {
	my $shared = shift;
	$self->lockfh_unset if $self->lockpid != $$;
	my $lockfh = $self->lockfh;
	unless(flock($lockfh, $shared ? LOCK_SH : LOCK_EX)) {
		my $lockname = $self->lockname;
		die "flock($lockname): $!\n";
	}
	return guard { flock($lockfh, LOCK_UN) };
}

=item dirs

Dummy function that accepts and discards the local and remote directory.

=cut

sub dirs {}

=item send_rpc

Sends an RPC call to the "out" file handle, using the appropriate locking
type depending on the size of the request. Throws an exception if an error
occurs.

=cut

sub send_rpc {
	confess("internal error: utf8 data passed") if utf8::is_utf8($_[1]);
	my $buf = pack('LC', length($_[1]), $_[0]).$_[1];
	if(length($buf) > PIPE_BUF) {
		my $lock = $self->guardlock;
		my $r = syswrite($self->out, $buf);
		die "write(): $!\n" unless defined $r;
		# POSIX guarantees that no partial writes will occur
		confess "short write" if $r < length($buf);
	} else {
		# POSIX guarantees that writes of up to PIPE_BUF bytes are atomic.
		# However, even if this write is atomic, we might interrupt a larger
		# non-atomic write. We therefore still have to take out a lock, but
		# it only has to be a shared one.
		my $lock = $self->guardlock(1);
		my $r = syswrite($self->out, $buf);
		die "write(): $!\n" unless defined $r;
		# POSIX guarantees that no partial writes will occur
		confess "short write" if $r < length($buf);
	}
}

# When sending an RPC message that requires a reply, we need to
# do locking on a higher level.
# In that case this non-locking version of send_rpc must be used.

sub send_rpc_unlocked {
	confess("internal error: utf8 data passed") if utf8::is_utf8($_[1]);
	my $buf = pack('LC', length($_[1]), $_[0]).$_[1];
	my $r = syswrite($self->out, $buf);
	die "write(): $!\n" unless defined $r;
	# POSIX guarantees that no partial writes will occur
	confess "short write" if $r < length($buf);
}

sub recv_rpc {
	my $in = $self->in;
	return saferead($in, unpack('L', saferead($in, 4)));
}

sub protocol_version {
	if(@_) {
		my $protocol_version = shift;
		$self->{protocol_version} = $protocol_version;
		$self->send_rpc(RSYNC_RPC_protocol_version, pack('L', $protocol_version));
		return;
	}
	confess("field protocol_version used uninitialized")
		unless exists $self->{protocol_version};
	return $self->{protocol_version};
}

sub checksumSeed {
	if(@_) {
		my $checksumSeed = shift;
		$self->{checksumSeed} = $checksumSeed;
		$self->send_rpc(RSYNC_RPC_checksumSeed, pack('L', $checksumSeed));
		return;
	}
	confess("field checksumSeed used uninitialized")
		unless exists $self->{checksumSeed};
	return $self->{checksumSeed};
}

sub attribGet {
	my $attrs = shift;
	my $lock = $self->guardlock;
	$self->send_rpc_unlocked(RSYNC_RPC_attribGet, serialize_attrs($attrs));
	my $res = $self->recv_rpc;
	return $res eq '' ? undef : parse_attrs($res);
}

sub fileDeltaRxStart {
	my ($attrs, $numblocks, $blocksize, $lastblocksize) = @_;
	$self->send_rpc(RSYNC_RPC_fileDeltaRxStart,
		pack('QLL', $numblocks, $blocksize, $lastblocksize).serialize_attrs($attrs));
}

sub fileDeltaRxNext {
	my ($blocknum, $data) = @_;
	if(defined $blocknum) {
		$self->send_rpc(RSYNC_RPC_fileDeltaRxNext_blocknum, pack('Q', $blocknum));
	} elsif(defined $data) {
		$self->send_rpc(RSYNC_RPC_fileDeltaRxNext_data, $data);
	}
	return 0;
}

sub fileDeltaRxDone {
	$self->send_rpc(RSYNC_RPC_fileDeltaRxDone, '');
	return undef;
}

sub csumStart {
	my ($attrs, $needMD4, $blockSize, $phase) = @_;
	$self->needMD4($needMD4);
	$self->send_rpc(RSYNC_RPC_csumStart, pack('LCC', $blockSize, $needMD4 ? 1 : 0, $phase).serialize_attrs($attrs));
	return $blockSize;
}

sub csumGet {
	my ($num, $csumLen, $blockSize) = @_;
	return unless $self->needMD4_isset;
	my $lock = $self->guardlock;
	$self->send_rpc_unlocked(RSYNC_RPC_csumGet, pack('QLC', $num, $blockSize, $csumLen));
	return $self->recv_rpc;
}

sub csumEnd {
	return unless $self->needMD4_isset;
	if($self->needMD4) {
		my $lock = $self->guardlock;
		$self->send_rpc_unlocked(RSYNC_RPC_csumEnd_digest, '');
		return $self->recv_rpc;
	} else {
		$self->send_rpc(RSYNC_RPC_csumEnd, '');
		return undef;
	}
}

sub attribSet {
	my ($attrs, $placeHolder) = @_;
	$self->send_rpc(RSYNC_RPC_attribSet, serialize_attrs($attrs));
	return undef;
}

use constant makeHardLink => undef;
use constant makePath => undef;
use constant makeSpecial => undef;
use constant ignoreAttrOnFile => undef;

use subs 'unlink';
sub unlink { return }

sub logHandlerSet { return }

sub statsGet {{}}

sub finish {
	$self->send_rpc(RSYNC_RPC_finish, '');
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

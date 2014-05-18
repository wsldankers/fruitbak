=encoding utf8

=head1 NAME

Fruitbak::Transfer::Rsync::IO - mediate between Fruitbak and File::RsyncP

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

package Fruitbak::Transfer::Rsync::IO;

use autodie;

use File::RsyncP::Digest;
use Data::Dumper;
use Fruitbak::Transfer::Rsync::RPC;
use Fruitbak::Transfer::Rsync::Lock;
use IO::File;
use POSIX qw(PIPE_BUF);

use Class::Clarity -self;

field needMD4;
field blockSize;
field preserve_hard_links;
field lockfile;
field lockfh;
field lockpid => 0;

use constant version => 2;

sub raiilock {
	my $pid = $$;
	return new Fruitbak::Transfer::Rsync::Lock(lockfh => $self->lockfh)
		if $self->lockfh_isset && $self->lockpid == $pid;
	my $lockfile = $self->lockfile;
	my $lockfh = new IO::File($lockfile, '+<')
		or die "can't open $lockfile: $!\n";
	$self->lockfh($lockfh);
	$self->lockpid($pid);
	return new Fruitbak::Transfer::Rsync::Lock(lockfh => $lockfh)
}

sub dirs {}

sub send_rpc {
	my $buf = pack('LC', length($_[1]), $_[0]).$_[1];
	if(length($buf) > PIPE_BUF) {
		my $lock = $self->raiilock;
		my $r = syswrite(\*STDOUT, $buf);
		# POSIX guarantees that no partial writes will occur
		die "short write\n"
			if $r < length($buf);
	} else {
		# POSIX guarantees that writes of up to PIPE_BUF bytes are atomic
		# so we don't need to acquire a lock here
		my $r = syswrite(\*STDOUT, $buf);
		# POSIX guarantees that no partial writes will occur
		die "short write\n"
			if $r < length($buf);
	}
}

sub send_rpc_unlocked {
	# When sending an RPC message that requires a reply, we need to
	# do locking on a higher level.
	# In that case this non-locking version of send_rpc must be used.
	confess("internal error: utf8 data passed to send_rpc()")
		if utf8::is_utf8($_[1]);
	my $buf = pack('LC', length($_[1]), $_[0]).$_[1];
	my $r = syswrite(\*STDOUT, $buf);
	# POSIX guarantees that no partial writes will occur
	die "short write\n"
		if $r < length($buf);
}

sub recv_rpc {
	return saferead(\*STDIN, unpack('L', saferead(\*STDIN, 4)));
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
	my $lock = $self->raiilock;
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
	$self->send_rpc(RSYNC_RPC_csumStart, pack('CLC', $needMD4 ? 1 : 0, $blockSize, $phase).serialize_attrs($attrs));
	return $blockSize;
}

sub csumGet {
	my ($num, $csumLen, $blockSize) = @_;
	return unless $self->needMD4_isset;
	my $lock = $self->raiilock;
	$self->send_rpc_unlocked(RSYNC_RPC_csumGet, pack('QLL', $num, $csumLen, $blockSize));
	return $self->recv_rpc;
}

sub csumEnd {
	return unless $self->needMD4_isset;
	if($self->needMD4) {
		my $lock = $self->raiilock;
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

{
	no warnings;
	sub unlink {}
}

sub logHandlerSet {}

sub statsGet {{}}

sub finish {
	$self->send_rpc(RSYNC_RPC_finish, '');
}

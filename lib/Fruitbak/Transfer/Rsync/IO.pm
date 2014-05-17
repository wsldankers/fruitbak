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
use IO::Handle;

use Class::Clarity -self;

field needMD4;
field blockSize;
field checksumSeed;
field protocol_version;
field preserve_hard_links;
use constant version => 2;

sub dirs {}

sub send {
	# TODO: custom buffering
	print pack('LC', length($_[1]), $_[0]), $_[1];
}

sub recv {
	STDOUT->flush;
	return saferead(unpack('L', saferead(4)));
}

# 1: (attrs) => (attrs)
sub attribGet {
	my $attrs = shift;
	$self->send(RSYNC_RPC_attribGet, serialize_attrs($attrs));
	my $res = $self->recv;
	return $res eq '' ? undef : parse_attrs($res);
}

# 2: (numblocks, blocksize, lastblocksize, attrs) => ()
sub fileDeltaRxStart {
	my ($attrs, $numblocks, $blocksize, $lastblocksize) = @_;
	$self->send(RSYNC_RPC_fileDeltaRxStart,
		pack('QLL', $numblocks, $blocksize, $lastblocksize).serialize_attrs($attrs));
}

# 3: (blocknum) => ()
# 4: (data) => ()
sub fileDeltaRxNext {
	my ($blocknum, $data) = @_;
	if(defined $blocknum) {
		$self->send(RSYNC_RPC_fileDeltaRxNext_blocknum, pack('Q', $blocknum));
	} elsif(defined $data) {
		$self->send(RSYNC_RPC_fileDeltaRxNext_data, $data);
	}
	return 0;
}

# 5: () => ()
sub fileDeltaRxDone {
	$self->send(5, '');
	return undef;
}

# 6: (needMD4, attrs) => ()
sub csumStart {
    my ($attrs, $needMD4) = @_;
	$self->needMD4($needMD4);
	$self->send(6, pack('C', $needMD4 ? 1 : 0).serialize_attrs($attrs));
}

# 7: (num, csumLen, blockSize) => (csumData)
sub csumGet {
	my ($num, $csumLen, $blockSize) = @_;
	return unless $self->needMD4_isset;
	$self->send(7, pack('QLL', $num, $csumLen, $blockSize));
	return $self->recv;
}

# 8: () => (digestData)
# 9: () => ()
sub csumEnd {
	return unless $self->needMD4_isset;
	if($self->needMD4) {
		$self->send(8, '');
		return $self->recv;
	}
	$self->send(9, '');
	return undef;
}

# 10: (attrs) => ()
sub attribSet {
	my ($attrs, $placeHolder) = @_;
	$self->send(10, serialize_attrs($attrs));
	return undef;
}

use constant makeHardLink => undef;
use constant makePath => undef;
use constant makeSpecial => undef;
use constant ignoreAttrOnFile => undef;

sub unlink($) {}

sub logHandlerSet {}

sub statsGet {{}}

# 0: () => ()
sub finish {
	$self->send(0, '');
}

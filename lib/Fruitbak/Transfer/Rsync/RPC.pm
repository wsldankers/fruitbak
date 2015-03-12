=encoding utf8

=head1 NAME

Fruitbak::Transfer::Rsync::RPC - mediate between Fruitbak and File::RsyncP

=head1 SYNOPSIS

 my ($len, $cmd) = unpack('LC', saferead($fh, 5));
 if($cmd == RSYNC_RPC_finish) {
 	last;
 } elsif($cmd == RSYNC_RPC_attribGet) {
 	my $attrs = my_handle_attribGet(parse_attrs($data));
 	print $fh serialize_attrs($attrs);
 } elseif(...) {
 	...
 }

=head1 DESCRIPTION

This package is used by Fruitbak::Transfer::Rsync to run File::RsyncP in
a separate process. To this end it defines a few utility functions for I/O
and marshalling purposes and it provides numbers for each of the methods of
File::RsyncP.

The reason to run File::RsyncP in a separate process is two-fold: first,
File::RsyncP expects to be able to use fork(), which wreaks havoc on any
open file, socket and database handles. Second, it allows for
parallelization of the rsync process and Fruitbak's own I/O.

The format of each command is 'LC' in pack syntax: first a 32 bit native
endian integer denoting the length of the arguments (not including the
command itself) followed by a single 8-bit integer that specifies the
command that is invoked. See the CONSTANTS section for the possible values.

Replies are sent as a 32 bit native endian integer denoting the length of
the reply, followed by the payload. Note that the RPC methods that expect
no reply should not be answered at all (not even with an empty reply).

See Fruitbak::Transfer::Rsync, Fruitbak::Transfer::Rsync::IO and
File::RsyncP for more information.

=cut

package Fruitbak::Transfer::Rsync::RPC;

use strict;
use warnings FATAL => 'all';

use Exporter qw(import);
use Carp qw(confess);

our @EXPORT = qw(
	saferead
	serialize_attrs
	parse_attrs

	RSYNC_RPC_finish
	RSYNC_RPC_attribGet
	RSYNC_RPC_fileDeltaRxStart
	RSYNC_RPC_fileDeltaRxNext_blocknum
	RSYNC_RPC_fileDeltaRxNext_data
	RSYNC_RPC_fileDeltaRxDone
	RSYNC_RPC_csumStart
	RSYNC_RPC_csumGet
	RSYNC_RPC_csumEnd_digest
	RSYNC_RPC_csumEnd
	RSYNC_RPC_attribSet
	RSYNC_RPC_protocol_version
	RSYNC_RPC_checksumSeed
	RSYNC_RPC_max
);
our @EXPORT_OK = (@EXPORT);

=head1 FUNCTIONS

=over

=item saferead($fh, $num)

Reads bytes from a file handle until the requested number of bytes is read.
The file handle must be blocking. Calls die() if an error is encountered.
EOF is considered an error.

=cut

sub saferead {
	my $fh = shift;
	my $num = shift;
	my $len = 0;
	my $res = '';
	
	while($len < $num) {
		my $r = sysread $fh, $res, $num - $len, $len;
		die "read(): $!\n" unless defined $r;
		confess("short read") unless $r;
		$len = length($res);
	}
	return $res;
}

=item serialize_attrs($attrs)

Given a file attribute hash as used by File::RsyncP, returns a string
representing that hash.

=cut

sub serialize_attrs {
	my $attrs = shift;
	return '' unless defined $attrs;
	return pack('(Z*)*', %$attrs);
}

=item parse_attrs($attrs)

Given a string as returned by serialize_attrs, returns a hash that is
identical to the input to serialize_attrs.

=cut

sub parse_attrs {
	my $attrs = shift;
	return undef if $attrs eq '';
	return { unpack('(Z*)*', $attrs) };
}

=back

=head1 CONSTANTS

These constants represent RPC calls as used between
Fruitbak::Transfer::Rsync and Fruitbak::Transfer::Rsync::IO (the wrapper
for File::RsyncP). They are sent by Fruitbak::Transfer::Rsync::IO;
Fruitbak::Transfer::Rsync parses the commands and sends replies.

Most of them are named after an eponymous File::RsyncP method; see the
File::RsyncP documentation for more information about their function.

=over

=item RSYNC_RPC_finish

This RPC code signals the end of the transfer. It has no arguments and
expects no reply.

=item RSYNC_RPC_attribGet

Arguments are an file attribute hash (see parse_attrs) and it expects a
file attribute hash in return (see serialize_attrs).

=item RSYNC_RPC_fileDeltaRxStart

Arguments are the number of blocks, the blocksize and the size of the last
block encoded as pack('QLL'), followed by an attribute.

=item RSYNC_RPC_fileDeltaRxNext_blocknum

Represents File::RsyncP's fileDeltaRxNext in the case where it has a
defined block number (which means that during a file transfer, a hash match
was found and the data can be copied from the existing file).
The argument is the block number encoded using pack('Q'). It expects no
answer.

=item RSYNC_RPC_fileDeltaRxNext_data

Represents File::RsyncP's fileDeltaRxNext in the case where it has new
data (which means that during a file transfer, no hash match was
found and the data must be transfered from the remote file).
The argument is the new data for the file. It expects no answer.

=item RSYNC_RPC_fileDeltaRxDone

This signals the end of the transfer of the current file. The file may be
closed and stored. Has no arguments and expects no reply.

=item RSYNC_RPC_csumStart

Arguments are the blocksize, a boolean indicating whether an MD4 hash is
requested, and the rsync transfer phase, encoded using pack('LCC'). Expects
no reply.

=item RSYNC_RPC_csumGet

Arguments are the number of checksums, the block size and the checksum
length, encoded using pack('QLC'). The reply should be the string
containing the checksums, just like File::RsyncP's csumGet expects it.

=item RSYNC_RPC_csumEnd_digest

Called in the case where csumEnd needs an MD4 checksum. Has no argument and
expects the MD4 digest in reply.

=item RSYNC_RPC_csumEnd

Called in the case where csumEnd does not need an MD4 checksum. Has no
argument and expects no reply.

=item RSYNC_RPC_attribSet

The only argument is the encoded File::RsyncP attribute hash. Expects no
reply.

=item RSYNC_RPC_protocol_version

The only argument is the current rsync protocol version number, encoded
using pack('L').

=item RSYNC_RPC_checksumSeed

The only argument is the current rsync checksum seed, encoded using
pack('L').

=item RSYNC_RPC_max

The highest RPC code you can expect. If you receive a higher value than
this, you can assume that the stream got desynchronised and that you should
abort the transfer.

=cut

use constant RSYNC_RPC_finish => 0;
use constant RSYNC_RPC_attribGet => 1;
use constant RSYNC_RPC_fileDeltaRxStart => 2;
use constant RSYNC_RPC_fileDeltaRxNext_blocknum => 3;
use constant RSYNC_RPC_fileDeltaRxNext_data => 4;
use constant RSYNC_RPC_fileDeltaRxDone => 5;
use constant RSYNC_RPC_csumStart => 6;
use constant RSYNC_RPC_csumGet => 7;
use constant RSYNC_RPC_csumEnd_digest => 8;
use constant RSYNC_RPC_csumEnd => 9;
use constant RSYNC_RPC_attribSet => 10;
use constant RSYNC_RPC_protocol_version => 11;
use constant RSYNC_RPC_checksumSeed => 12;
use constant RSYNC_RPC_max => RSYNC_RPC_checksumSeed;

1;

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

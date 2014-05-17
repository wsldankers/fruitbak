=encoding utf8

=head1 NAME

Fruitbak::Transfer::Rsync::RPC - mediate between Fruitbak and File::RsyncP

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

package Fruitbak::Transfer::Rsync::RPC;

use Exporter qw(import);

our @EXPORT = qw(saferead serialize_attrs parse_attrs
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
);
our @EXPORT_OK = (@EXPORT);

use strict;
use warnings FATAL => 'all';
use autodie;

sub saferead {
	my $num = shift;
	my $len = 0;
	my $res = '';
	
	while($len < $num) {
		read STDIN, $res, $num - $len, $len
			or die "Short read\n";
		$len = length($res);
	}
	return $res;
}

sub serialize_attrs {
	my $attrs = shift;
	return join("\0", %$attrs);
}

sub parse_attrs {
	my %attrs = unpack('(Z*)*', shift);
	return \%attrs;
}

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

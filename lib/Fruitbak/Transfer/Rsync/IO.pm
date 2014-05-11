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

use Class::Clarity -self;
use Data::Dumper;

field fbak => sub { $self->host->fbak };
field pool => sub { $self->fbak->pool };
field host => sub { $self->backup->host };
field backup => sub { $self->share->backup };
field share => sub { $self->xfer->share };
field refShare => sub { $self->share->refShare };
field xfer;
field curfile;
field reffile => undef;

field blockSize;
field checksumSeed;
field protocol_version;
field preserve_hard_links;
use constant version => 2;

sub dirs {
	my ($localDir, $remoteDir) = @_;
}

sub attribGet {
	my $attrs = shift;
	my $ref = $self->refShare;
	return unless $ref;
	return $ref->get_entry($attrs->{name}, 1);
}

sub create_dentry {
	my $attrs = shift;
	return new Fruitbak::Dentry(
		name => $attrs->{name},
		mode => $attrs->{mode},
		size => $attrs->{size},
		mtime => $attrs->{mtime},
		uid => $attrs->{uid},
		gid => $attrs->{gid},
		@_
	);
}

sub fileDeltaRxStart {
	my ($attrs, $numblocks, $blocksize, $lastblocksize) = @_;
	$self->curfile(new Class::Clarity(
		attrs => $attrs,
		numblocks => $numblocks,
		blocksize => $blocksize,
		lastblocksize => $lastblocksize,
		poolwriter => $self->pool->writer,
	));
	if(my $refShare = $self->refShare) {
		$self->reffile($refShare->get_entry($attrs->{name}, 1));
	}
}

sub fileDeltaRxNext {
	my ($blocknum, $data) = @_;
	my $curfile = $self->curfile;
	unless(defined $data) {
		return unless defined $blocknum;
		my $reffile = $self->reffile;
		die "No reffile but \$data undef? blocknum=$blocknum\n"
			unless defined $reffile;
		my $blocksize = $curfile->blocksize;
		$data = $reffile->pread($blocknum * $blocksize, $blocksize);
		warn "Copied block $blocknum.\n";
	}
	$curfile->poolwriter->write($data);
	return 0;
}

sub fileDeltaRxDone {
	my $curfile = $self->curfile;
	my ($hashes, $size) = $curfile->poolwriter->close;
	my $dentry = $self->create_dentry($curfile->attrs, extra => $hashes, size => $size);
	$self->share->add_entry($dentry);
	$self->curfile_reset;
	return undef;
}

sub attribSet {
	my ($attrs, $placeHolder) = @_;
	if(my $refShare = $self->refShare) {
		if(my $dentry = $refShare->get_entry($attrs->{name})) {
			$self->share->add_entry($dentry);
			return undef;
		}
	}
	my $dentry = $self->create_dentry($attrs);
	$self->share->add_entry($dentry);
	return undef;
}

use constant makePath => undef;
use constant makeSpecial => undef;
use constant unlink => undef;

use constant ignoreAttrOnFile => undef;

sub logHandlerSet {}

sub statsGet {{}}

sub finish {
	$self->share->finish;
}

sub DESTROY {}

sub AUTOLOAD {
	our $AUTOLOAD;
	die "'$AUTOLOAD' not yet implemented\n";
}

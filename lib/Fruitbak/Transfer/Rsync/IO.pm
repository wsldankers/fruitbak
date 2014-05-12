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

use File::RsyncP::Digest;
use Data::Dumper;

use Class::Clarity -self;

field fbak => sub { $self->host->fbak };
field pool => sub { $self->fbak->pool };
field host => sub { $self->backup->host };
field backup => sub { $self->share->backup };
field share => sub { $self->xfer->share };
field refShare => sub { $self->share->refShare };
field xfer;
field curfile;
field reffile => undef;
field digest => sub { new File::RsyncP::Digest($self->protocol_version) };
field csumDigest => sub { new File::RsyncP::Digest($self->protocol_version) };

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
	my $dentry = new Fruitbak::Dentry(
		name => $attrs->{name},
		mode => $attrs->{mode},
		size => $attrs->{size},
		mtime => $attrs->{mtime},
		uid => $attrs->{uid},
		gid => $attrs->{gid},
		@_
	);
	warn Dumper($attrs) if $attrs->{name} =~ m{/extra/};
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

sub setup_reffile {
	my $attrs = shift;
	if(my $refShare = $self->refShare) {
		if(my $dentry = $refShare->get_entry($attrs->{name}, 1)) {
warn "$attrs->{name} links to $dentry->{extra}\n" if $dentry->is_hardlink;
			if($dentry->is_file) {
				$self->reffile(new Class::Clarity(
					attrs => $attrs,
					dentry => $dentry,
					poolreader => $self->pool->reader(digests => $dentry->extra),
				));
			}
		}
	}
}

sub fileDeltaRxStart {
	my ($attrs, $numblocks, $blocksize, $lastblocksize) = @_;
	my $pool = $self->pool;
	$self->curfile(new Class::Clarity(
		attrs => $attrs,
		numblocks => $numblocks,
		blocksize => $blocksize,
		lastblocksize => $lastblocksize,
		poolwriter => $pool->writer,
	));
	$self->setup_reffile($attrs);
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
		$data = $reffile->poolreader->pread($blocknum * $blocksize, $blocksize);
#		warn "Copied block $blocknum.\n";
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

sub csumStart {
    my ($attrs, $needMD4) = @_;

	$self->csumEnd if $self->reffile;

	my $refShare = $self->refShare or return;
	my $dentry = $refShare->get_entry($attrs->{name}, 1) or return;
	return unless $dentry->is_file;

	$self->setup_reffile($attrs);

	$self->csumDigest_reset;
	$self->csumDigest->add(pack('V', $self->checksumSeed))
		if $needMD4;
}

sub csumGet {
	return unless $self->reffile_isset;

	my ($num, $csumLen, $blockSize) = @_;

	$num ||= 100;
	$csumLen ||= 16;

	my $data = $self->reffile->poolreader->read($blockSize * $num);

	if($self->csumDigest_isset) {
		$self->csumDigest->add($data);
	}

	return $self->digest->blockDigest($data, $blockSize, $csumLen, $self->checksumSeed);
}

sub csumEnd {
	return unless $self->reffile_isset;
	my $reffile = $self->reffile;
	$self->reffile_reset;
	#
	# make sure we read the entire file for the file MD4 digest
	#
	if($self->csumDigest_isset) {
		my $csumDigest = $self->csumDigest;
		for(;;) {
			my $data = $self->reffile->poolreader->read;
			last if $data eq '';
			$csumDigest->add($data);
		}
		return $csumDigest->digest;
	}
	return undef;
}

sub attribSet {
	my ($attrs, $placeHolder) = @_;
	warn "attribSet($attrs->{name})";
	if(my $refShare = $self->refShare) {
		if(my $dentry = $refShare->get_entry($attrs->{name}, 1)) {
			$self->share->add_entry($dentry);
			return undef;
		}
	}
	my $dentry = $self->create_dentry($attrs);
	$self->share->add_entry($dentry);
	return undef;
}

sub makeHardLink {
	my ($attrs, $isEnd) = @_;
	return unless $isEnd;
	my $dentry = $self->create_dentry($attrs, hardlink => $attrs->{hlink});
	$self->share->add_entry($dentry);
	warn "makeHardLink($attrs->{name}, $attrs->{hlink})\n";
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

=encoding utf8

=head1 NAME

Fruitbak::Command::Tar - implementation of CLI tar command

=head1 AUTHOR

Wessel Dankers <wsl@fruit.je>

=head1 COPYRIGHT

Copyright (c) 2014 Wessel Dankers <wsl@fruit.je>

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

package Fruitbak::Command::Tar;

no utf8;

use Fruitbak::Command -self;

use IO::Handle;
use Fcntl qw(:mode);
use Symbol qw(qualify_to_ref);

BEGIN {
	$Fruitbak::Command::commands{tar} = [__PACKAGE__, "Write a tar file to stdout"];
}

field fh => sub { qualify_to_ref(select) };
field curfile;

sub format8 {
	foreach(@_) {
		return sprintf("%07o\0", $_)
			if $_ < (1 << 21);
		return "\x80".substr(pack('Q>', $_), 1)
			if $_ < (1 << 56);
	}
}

sub format12 {
	my $value = shift;
	return sprintf("%011o\0", $value)
		if $value < (1 << 33);
	return "\x80\0\0\0".pack('Q>', $value);
}

sub output_header {
	my ($name, $mode, $uid, $gid, $size, $mtime, $type, $link, $maj, $min) = @_;
	my $header = pack('a100 a8 a8 a8 a12 a12 a8 a a100 a8 a32 a32 a8 a8 a155 a12',
		$name,
		$self->format8($mode),
		$self->format8($uid, 65534),
		$self->format8($gid, 65534),
		$self->format12($size),
		$self->format12($mtime),
		'        ', # checksum placeholder
		$type,
		($link // ''),
		"ustar  ", # magic + version
		'', # username
		'', # groupname
		($maj // ''),
		($min // ''),
		'', # prefix
		'', # padding
	);

	my $csum = 0;
	foreach(unpack('C*', $header)) {
		$csum += $_;
	}
	substr($header, 148, 7, sprintf('%06o', $csum)."\0");

	my $fh = $self->fh;
	print $fh $header
		or die "write(): $!\n";
}

my @filetypes;
$filetypes[S_IFREG] = 0;
$filetypes[S_IFLNK] = 2;
$filetypes[S_IFCHR] = 3;
$filetypes[S_IFBLK] = 4;
$filetypes[S_IFDIR] = 5;
$filetypes[S_IFIFO] = 6;

sub output_dentry {
	my ($dentry, $hardlink) = @_;

	my $mode = $dentry->mode;

	my $type = $filetypes[$mode & S_IFMT];
	return undef unless defined $type;
	$type = 1 if defined $hardlink;

	my $name = $dentry->name;
	$name = '.' if $name eq '';
	$name .= '/' if $type == 5;

	my $linkname = $type == 1 ? $hardlink
		: $type == 2 ? $dentry->symlink
		: '';

	my $fh;
	if(length($linkname) > 100) {
		my $size = length($linkname) + 1;
		$self->output_header('././@LongLink', 0644, 0, 0, $size, 0, 'K');
		$fh //= $self->fh;
		print $fh $linkname, "\0" x ((-$size & 511) + 1)
			or die "write(): $!\n";
	}
	if(length($name) > 100) {
		my $size = length($name) + 1;
		$self->output_header('././@LongLink', 0644, 0, 0, $size, 0, 'L');
		$fh //= $self->fh;
		print $fh $name, "\0" x ((-$size & 511) + 1)
			or die "write(): $!\n";
	}

	my @dev;
	if($dentry->is_device) {
		my $major = $self->format8($dentry->rdev_major);
		return undef unless defined $major;
		my $minor = $self->format8($dentry->rdev_minor);
		return undef unless defined $minor;
		@dev = ($major, $minor);
	}

	$self->output_header(
		$name,
		$dentry->mode & 07777,
		$dentry->uid,
		$dentry->gid,
		$dentry->storedsize,
		$dentry->mtime,
		$type,
		$linkname,
		@dev,
	);

	return 1;
}

sub start_file {
	confess("start_file called when a file is still in progress")
		if $self->curfile_isset;
	my $dentry = shift;
	confess("start_file called on something that is not a file")
		unless $dentry->is_file;
	$self->curfile($dentry);
	return $self->output_dentry($dentry);
}

sub end_file {
	confess("end_file called when no file is in progress")
		unless $self->curfile_isset;
	my $curfile = $self->curfile;
	$self->curfile_reset;
	my $size = $curfile->storedsize;
	my $fh = $self->fh;
	print $fh "\0"x(-$size & 511)
		or die "write(): $!\n";
	return;
}

sub output_entry {
	confess("output_entry called when a file is still in progress")
		if $self->curfile_isset;
	my ($dentry, $hardlink) = @_;
	confess("output_entry called on a file")
		if !defined $hardlink && $dentry->is_file;
	return $self->output_dentry(@_);
}

sub finish {
	confess("finish called when a file is still in progress")
		if $self->curfile_isset;
	my $fh = $self->fh;
	print $fh "\0"x5120
		or die "write(): $!\n";
}

sub run {
	my (undef, $hostname, $backupnum, $sharename, $path) = @_;

	die "usage: fruitbak tar <hostname> <backup> <share> <path>\n"
		unless defined $sharename;

	my $fh = $self->fh;
	die "refusing to write a binary file to a terminal\n"
		if -t $fh;
	binmode $fh;

	my $fbak = $self->fbak;

	my $host = $fbak->get_host($hostname);
	my $backup = $host->get_backup($backupnum);
	my $share;
	if(defined $path) {
		$share = $backup->get_share($sharename);
	} else {
		($share, $path) = $backup->resolve_share($sharename);
	}
	my $cursor = $share->find($path);
	my %remap;
	# Tricky: we need both the root node (which we can only fetch now) and the
	# first node. However, we need to iterate over both of them (though $root may
	# be undef) and only then continue with the other, normal nodes.
	my $root = $cursor->read;
	my $first = $cursor->fetch;
	my $firstinode = $first->original->inode if $first;
	for(my $dentry = $root || $first; $dentry; $dentry = $root ? do { undef $root; $first } : $cursor->fetch) {
		my $name = $dentry->name;
		my $remapped = $remap{$name};
		if(defined $remapped) {
			$self->output_entry($dentry, $remapped);
			next;
		}
		if($dentry->is_hardlink) {
			my $originalinode = $dentry->original->inode;
			my $target = $dentry->target;
			my $targetname = $target->name;
			my $targetinode = $target->inode;
			if($targetinode >= $firstinode && $targetinode < $originalinode) {
				# target is already output
				$self->output_entry($dentry, $targetname);
				next;
			}
			$remap{$targetname} = $name;
			# fall-through: this hardlink will be dumped as a regular file
		}
		if($dentry->is_file) {
			$self->start_file($dentry)
				or confess("regular entry refused?");

			my $reader = $fbak->pool->reader(digests => $dentry->digests);

			my $buf = $reader->read;
			while($$buf ne '') {
				print $fh $$buf
					or die "write(): $!\n";
				$buf = $reader->read;
			}
			$self->end_file;
		} else {
			$self->output_entry($dentry);
		}
	}

	$self->finish;

	return 0;
}

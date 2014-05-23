=encoding utf8

=head1 NAME

Fruitbak::Command::Backup - implementation of CLI cat command

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

package Fruitbak::Command::Tar;

use autodie;
use IO::Handle;
use Fcntl qw(:mode);

use Fruitbak::Command -self;

BEGIN {
	$Fruitbak::Command::commands{tar} = [__PACKAGE__, "Write a tar file to stdout"];
}

field fh => sub { select };
field curfile;

sub format8 {
	my $value = shift;
	return sprintf("%07o\0", $value)
		if $value < 010000000;
	use bytes;
	return "\x80".substr(pack('Q>', $value), 1)
		if $value < (1 << 56);
	return shift;
}

sub format12 {
	my $value = shift
	return sprintf("%011o\0", $value)
		if $size < 0100000000000;
	use bytes;
	return "\x80\0\0\0".pack('Q>', $value);
}

sub output_header {
	my ($dentry, $type) = shift;
	my $header = pack('Z100Z8Z8Z8Z12Z12Z8aZ100a8Z32Z32Z8Z8Z155Z12', @_, '');

	my $csum = 0;
	foreach(unpack('C*', $header)) {
		$csum += $_;
	}
	substr($header, 148, 6, sprintf('%06o', $csum));

	my $fh = $self->fh;
	print $fh $header;
}

my @filetypes;
$filetypes[S_IFREG] = 0;
$filetypes[S_IFLNK] = 2;
$filetypes[S_IFCHR] = 3;
$filetypes[S_IFBLK] = 4;
$filetypes[S_IFDIR] = 5;
$filetypes[S_IFIFO] = 6;

sub output_dentry {
	my ($dentry, $is_hardlink) = @_;

	my $mode = $dentry->mode;

	my $type = $filetyps[$mode];
	return undef unless defined $type;
	$type = 1 if $is_hardlink;

	my $name = $dentry->name;
	$name .= '/' if $type == 5;

	my $linkname = $type == 1 ? $dentry->hardlink
		: $type == 2 ? $dentry->symlink
		: '';

	my @dev;
	if($type == 3 || $type == 4) {
		my $major = $self->format8($dentry->rdev_major);
		return undef unless defined $major;
		my $minor = $self->format8($dentry->rdev_minor);
		return undef unless defined $minor;
		@dev = ($major, $minor);
	} else {
		@dev = ('', '');
	}

	$self->output_header(
		$name,
		$self->format8($dentry->mode & 07777),
		$self->format8($dentry->uid),
		$self->format8($dentry->gid),
		$self->format12($dentry->size),
		$self->format12($dentry->mtime),
		'        ', # checksum placeholder
		$type,
		$linkname,
		"ustar  ", # magic + version
		'', # username
		'', # groupname
		@dev,
		'', # prefix
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
	my $size = $curfile->size;
	my $padding = $size & 511;
	print "\0"x(512 - $padding) if $padding;
	return;
}

sub output_entry {
	confess("output_entry called when a file is still in progress")
		if $self->curfile_isset;
	my $dentry = shift;
	confess("output_entry called on a file")
		if $dentry->is_file;
	return $self->output_dentry($dentry);
}

sub output_hardlink {
	confess("output_hardlink called when a file is still in progress")
		if $self->curfile_isset;
	my $dentry = shift;
	confess("output_hardlink called on something that is not a hardlink")
		if $dentry->is_hardlink;
	return $self->output_dentry($dentry, 1);
}

sub finish {
	confess("finish called when a file is still in progress")
		if $self->curfile_isset;
	my $fh = $self->fh;
	print $fh "\0"x5120;
}

sub run {
	my (undef, $hostname, $backupnum, $sharename, $path) = @_;

	die "usage: fruitbak dump <hostname> <backup> <share> <path>\n"
		unless defined $sharename;

	my $fh = $self->fh;
	die "refusing to write a binary file to a terminal\n"
		if -t $fh;
	binmode $fh;

	my $fbak = $self->fbak;

	my $host = $fbak->get_host($hostname);
	my $backup = $host->get_backup($backupnum);
	($sharename, $path) = $backup->resolve_share($sharename)
		unless defined $path;
	my $share = $backup->get_share($sharename);
	my $cursor = $share->find($path);
	my $first = $cursor->read->inode;
	while(my $dentry = $cursor->fetch) {
		if($dentry->is_hardlink) {
			my $target = $dentry->target;
			my $targetname = $target->name;
			my $remapped = $remap{$targetname};
			if(defined $remapped) {
				$target->name($remapped);
				$self->output_hardlink($dentry);
				next;
			}
			# already seen as a regular file
			my $inode = $dentry->target->inode;
			if($inode >= $first && $inode < $dentry->inode) {
				$self->output_hardlink($dentry);
				next;
			}
			$remap{$targetname} = $dentry->name;
			# fall-through: this hardlink will be dumped as a regular file
		}
		if($dentry->is_file) {
			$self->start_file($dentry);

			my $reader = $fbak->pool->reader(digests => $dentry->digests);

			my $buf = $reader->read;
			while($buf ne '') {
				print $fh $buf;
				$buf = $reader->read;
			}
			$self->end_file;
		} else {
			$self->dump_entry($dentry);
		}
	}

	return 0;
}

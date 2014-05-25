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
no utf8;

use IO::Handle;
use Fcntl qw(:mode);

use Fruitbak::Command -self;

BEGIN {
	$Fruitbak::Command::commands{tar} = [__PACKAGE__, "Write a tar file to stdout"];
}

field fh => sub {
	my $fh = select;
	return ref $fh ? $fh : eval "\*$fh";
};
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
	my $header = pack('Z100 Z8 Z8 Z8 Z12 Z12 a8 a Z100 a8 Z32 Z32 Z8 Z8 Z155 Z12',
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
		print $fh $linkname, "\0" x ((-$size & 511) + 1);
	}
	if(length($name) > 100) {
		my $size = length($name) + 1;
		$self->output_header('././@LongLink', 0644, 0, 0, $size, 0, 'L');
		$fh //= $self->fh;
		print $fh $name, "\0" x ((-$size & 511) + 1);
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
	print $fh "\0"x(-$size & 511);
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
	my %remap;
	my $dentry = $cursor->read;
	my $first = $cursor->fetch;
	for($dentry //= $first; $dentry; $dentry = $cursor->fetch) {
		my $name = $dentry->name;
		my $inode = $dentry->inode;
		my $remapped = $remap{$name};
		if(defined $remapped) {
			$self->output_entry($dentry, $remapped);
			next;
		}
		if($dentry->is_hardlink) {
			my $target = $dentry->target;
			my $targetname = $target->name;
			my $remapped = $remap{$targetname};
			# already seen as a regular file
			my $targetinode = $target->inode;
			if($targetinode >= $first->inode && $targetinode < $inode) {
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
				print $fh $$buf;
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

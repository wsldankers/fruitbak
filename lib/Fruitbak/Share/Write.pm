=encoding utf8

=head1 NAME

Fruitbak::Share::Write - write a share of a new backup

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

package Fruitbak::Share::Write;

use Class::Clarity -self;

use Fcntl qw(:mode);
use Carp qw(confess);
use File::Hardhat::Maker;

use Fruitbak::Share::Format;
use Fruitbak::Pool::Write;
use Fruitbak::Transfer::Rsync;

field name;
field dir => sub { $self->backup->sharedir . '/' . mangle($self->name) };
field fbak => sub { $self->backup->fbak };
field backup;
field refbackup => sub { $self->backup->refbackup };
field refshare => sub {
    my $refbak = $self->refbackup;
    return undef unless $refbak;
    return $refbak->get_share($self->name); 
};
field hhm => sub {
	my $dir = $self->dir;

	mkdir($dir) or $!{EEXIST}
		or die "mkdir($dir): $!\n";

	return new File::Hardhat::Maker("$dir/metadata.hh");
};

# add a Fruitbak::Dentry to the database
# the extra field must be filled depending on the type of the dentry:
# - regular files: the concatenated hashes that make up the file contents
# - symlinks: the symlink target (use $dentry->symlink)
# - hardlinks: the hardlink target (the path, not the inode - use $dentry->hardlink)
# - device nodes: the device number encoded using $dentry->rdev
# - directories, fifos and sockets: leave empty
sub add_entry {
	my $dentry = shift;
	$self->hhm->add($dentry->name, attrformat($dentry));
}

sub run {
	my $xfer = new Fruitbak::Transfer::Rsync(share => $self);
	$xfer->recv_files;
}

# finish the share and convert this object to a Fruitbak::Share::Read
# FIXME: register this object with the backup object it belongs to
sub finish {
	my $hhm = $self->hhm;
	$self->hhm_reset;
	$hhm->parents;
	$hhm->finish;
	bless $self, 'Fruitbak::Share::Read';
}

=encoding utf8

=head1 NAME

Fruitbak::Share::Read - Fruitbak class that represents a backed up share

=head1 SYNOPSIS

 my $fbak = new Fruitbak(confdir => '/etc/fruitbak');
 my $host = $fbak->get_host('pikachu');
 my $backup = $host->get_backup(3);
 my $share = $host->get_share('root');

=head1 DESCRIPTION

Use this class to access existing backup shares. You can query metadata,
list files and get Fruitbak::Dentry objects of specific files.

As with all Fruitbak classes, any errors will throw an exception (using
‘die’). Use eval {} as required.

=head1 CONSTRUCTOR

The only required arguments are backup and name, but don't call this
constructor directly. Always use $backup->get_share.

=cut

package Fruitbak::Share::Read;

use Class::Clarity -self;

use Fcntl qw(:mode);
use IO::File;
use JSON;
use File::Hardhat;
use File::Hashset;
use Fruitbak::Share::Cursor;
use Fruitbak::Share::Format;
use Fruitbak::Pool::Read;
use Fruitbak::Dentry;
use Fruitbak::Dentry::Hardlink;

=head1 FIELDS

Fruitbak makes heavy use of Class::Clarity (the base class of this class).
One of the features of Class::Clarity is ‘fields’: elements of the object
hash with eponymous getters and setters. These fields can optionally have
an initializer: a function that is called when no value is yet assigned to
the hash element. For more information, see L<Class::Clarity>.

=over

=item field dir

The path to the directory that contains the files of this share. Do not
set.

=cut

field dir => sub { $self->backup->sharedir . '/' . mangle($self->name) };

=item field hh

The File::Hardhat object that contains the filesystem metadata of this
share. For internal use only.

=cut

field hh => sub { new File::Hardhat($self->dir . '/metadata.hh') };

=item field backup

The Fruitbak::Backup object to which this share belongs. Set during
initialization, do not modify.

=cut

field backup;

=item field name

The name of this backup. See also the mountpoint and path properties. Do
not modify.

=cut

field name;

=item field fbak

The Fruitbak object that is the root ancestor of this backup share. Do not
set.

=cut

field fbak => sub { $self->backup->fbak };

=item field info

The metadata for this share, in the form in which it is loaded from disk.
For internal use only.

=cut

field info => sub {
	my $dir = $self->dir;
	my $info = new IO::File("$dir/info.json", '<')
		or die "open($dir/info.json): $!\n";
	my $json = do { local $/; <$info> };
	$info->eof or die "read($dir/info.json): $!\n";
	$info->close;
	return decode_json($json);
};

=item field path

The filesystem path on the client system where this share was backed up.
Note that this does not have to be the same as the place where the data is
normally found. See the mountpoint field for that. Do not set.

=cut

field path => sub { $self->info->{path} };

=item field mountpoint

The path on the client system where this share is normally found. Does not
have to be the same path as where the backup data was actually retrieved
from (for instance, if the data was retrieved from a snapshot).

=cut

field mountpoint => sub { $self->info->{mountpoint} };

=item field startTime

The starting time of this backup share, in seconds since the unix epoch. Do
not set.

=cut

field startTime => sub { $self->info->{startTime} };

=item field endTime

The ending time of this backup share, in seconds since the unix epoch. Do
not set.

=cut

field endTime => sub { $self->info->{endTime} };

=item field error

If a fatal error occurs during the backup, the error message is available
here. If no fatal error occurred, it is undef.

=cut

field error => sub { $self->info->{error} };

=item field hashes

A File::Hashset object representing the digests of all files of this
share. See L<Fruitbak(7)> for more information about how digests are used
in Fruitbak.

=back

=cut

field hashes => sub {
	my $hashes = $self->dir . '/hashes';
	unless(-e $hashes) {
		open my $fh, '>:raw', "$hashes.new"
			or die "open($hashes.new): $!\n";
		my $c = $self->hh->find('');
		my $data = $c->read;
		(undef, $data) = $c->fetch
			unless defined $data;
		while(defined $data) {
			my $hashes = just_the_hashes($data);

			print $fh $hashes
				or die "write($hashes.new): $!\n";

			(undef, $data) = $c->fetch;
		}
		$fh->flush or die "write($hashes.new): $!\n";
		$fh->sync or die "fsync($hashes.new): $!\n";
		$fh->close or die "close($hashes.new): $!\n";
		undef $fh;
		File::Hashset->sortfile("$hashes.new", $self->fbak->pool->hashsize);
		rename("$hashes.new", $hashes);
	}
	return File::Hashset->load($hashes);
};

=head1 METHODS

=over

=item ls($path)

Start a shallow directory listing of a path in this share. In scalar
context, returns a Fruitbak::Share::Cursor object that can be used to
retrieve a Fruitbak::Dentry of the entry itself and any entries directly
underneath it. In list context it returns the names of the entries directly
underneath the requested path. These names include the parent path.

=cut

sub ls {
	my $path = shift;
	my $c = $self->hh->ls($path);
	return new Fruitbak::Share::Cursor(share => $self, hhcursor => $c)
		unless wantarray;
	my @res;
	for(;;) {
		my $name = $c->fetch;
		last unless defined $name;
		push @res, $name;
	}
	return @res;
}

=item find($path)

Start a recursive directory listing of a path in this share. In scalar
context, returns a Fruitbak::Share::Cursor object that can be used to
retrieve a Fruitbak::Dentry of the entry itself and any entries anywhere
underneath it. In list context it returns the names of the entries anywhere
underneath the requested path. These names include the parent path.

=cut

sub find {
	my $path = shift;
	my $c = $self->hh->find($path);
	return new Fruitbak::Share::Cursor(share => $self, hhcursor => $c)
		unless wantarray;
	my @res;
	for(;;) {
		my $name = $c->fetch;
		last unless defined $name;
		push @res, $name;
	}
	return @res;
}

=item get_entry($path)

Given a path, returns a Fruitbak::Dentry object for that path, or undef if
it doesn't exist. If the entry is a hardlink, it will actually return a
Fruitbak::Dentry::Hardlink object as convenience.

=back

=cut

sub get_entry {
	my $path = shift;
	my $hh = $self->hh;
	my ($name, $data, $inode) = $hh->get($path)
		or return undef;
	my $dentry = attrparse($data, name => $name, inode => $inode);

	if($dentry->is_hardlink) {
		($name, $data, $inode) = $hh->get($dentry->hardlink);
		my $target = attrparse($data, name => $name, inode => $inode);
		return new Fruitbak::Dentry::Hardlink(original => $dentry, target => $target);
	}

	return $dentry;
}

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

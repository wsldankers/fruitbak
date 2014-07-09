=encoding utf8

=head1 NAME

Fruitbak::Backup::Read - Fruitbak class to access a specific backup of a host

=head1 SYNOPSIS

 my $fbak = new Fruitbak(confdir => '/etc/fruitbak');
 my $host = $fbak->get_host('pikachu');
 my $backup = $host->get_backup(3);

=head1 DESCRIPTION

Use this class to access existing backups. You can query metadata, list
shares and get handles to specific shares.

As with all Fruitbak classes, any errors will throw an exception (using
‘die’). Use eval {} as required.

=head1 CONSTRUCTOR

The only required arguments are host and number, but don't call this
constructor directly. Always use $host->get_backup.

=cut

package Fruitbak::Backup::Read;

use Class::Clarity -self;

use IO::File;
use IO::Dir;
use JSON;
use Scalar::Util qw(weaken);
use File::Hashset;
use Fruitbak::Util;
use Fruitbak::Share::Read;
use Fruitbak::Share::Format;

=head1 FIELDS

Fruitbak makes heavy use of Class::Clarity (the base class of this class).
One of the features of Class::Clarity is ‘fields’: elements of the object
hash with eponymous getters and setters. These fields can optionally have
an initializer: a function that is called when no value is yet assigned to
the hash element. For more information, see L<Class::Clarity>.

=over

=item field fbak

The Fruitbak object that this object (and its host) belong to. Do not set.

=cut

field fbak => sub { $self->host->fbak };

=item field host

The host object that this backup belongs to. Should be set before calling
any methods ($host->get_backup will take of that). Do not modify afterwards.

=cut

field host;

=item field number

The unique (within the scope of the host) number of this backup. Do not
change after calling any methods.

=cut

field number;

=item field dir

The directory path of this backup. Do not set.

=cut

field dir => sub { $self->host->dir . '/' . $self->number };

=item field sharedir

The path to the directory containing the shares of this backup.
Do not modify.

=cut

field sharedir => sub { $self->dir . '/share' };

=item field shares_cache

A hash containing weak references to the Fruitbak::Share::Read objects
that are in use. For internal use only.

=cut

field shares_cache => {};

=item field info

The metadata that was stored with this backup. For internal use only.

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

=item field failed

Indicates the status of the backup, as it was recorded when the backup
finished. If this value is ‘true’, it means one of more of the shares
failed fatally during the backup. Do not set.

=cut

field failed => sub { $self->info->{failed} ? !undef : !!undef };

=item field level

Integer indicating the ‘level’ of the backup, which is the number of steps
it takes to traverse the chain of reference backups until you reach a full
backup. Do not set.

=cut

field level => sub { $self->info->{level} };

=item field full

Boolean indicating whether this is a full or incremental backup.

=cut

field full => sub { !$self->level };

=item field startTime

The starting time of this backup, in seconds since the unix epoch. Do not
set.

=cut

field startTime => sub { $self->info->{startTime} };

=item field endTime

The ending time of this backup, in seconds since the unix epoch. Do not
set.

=cut

field endTime => sub { $self->info->{endTime} };

=item field hashes

A File::Hashset object representing the digests of all shares of this
backup. See L<Fruitbak(7)> for more information about how digests are used
in Fruitbak.

=cut

field hashes => sub {
	my $hashes = $self->dir . '/hashes';
	unless(-e $hashes) {
		File::Hashset->merge("$hashes.new", $self->fbak->pool->hashsize,
			map { $self->get_share($_)->hashes } @{$self->shares});
		rename("$hashes.new", $hashes)
			or die "rename($hashes.new, $hashes): $!\n";
	}
	return File::Hashset->load($hashes);
};

=item shares

The list of names of the shares for this backup. Returned as an array
reference. Do not set.

=back

=cut

field shares => sub {
	my $sharedir = $self->sharedir;
	my $fh = new IO::Dir($sharedir)
		or die "open($sharedir): $!\n";
	my @shares =
		sort
		map { unmangle($_) }
		grep { !/^\.{1,2}$/ }
		$fh->read;
	return \@shares;
};

=head1 METHODS

=over

=item get_share($sharename)

Given a name, returns a Fruitbak::Share::Read object. Ensures that no two
Fruitbak::Share::Read objects refer to the same share.

=cut

sub get_share {
	my $name = shift;
	my $cache = $self->shares_cache;
	my $share = $cache->{$name};
	unless(defined $share) {
		$share = new Fruitbak::Share::Read(backup => $self, name => $name);
		$cache->{$name} = $share;
		weaken($cache->{$name});
	}
	return $share;
}

=item share_exists($sharename)

Given a name, returns a boolean indicating whether this share exists.

=cut

sub share_exists {
	my $name = shift;
	return 1 if $self->shares_cache->{$name};
	my $sharedir = $self->sharedir;
	return !!lstat($sharedir.'/'.mangle($name));
}

=item resolve_share($path)

Given a path, will look for the share of this backup that has the longest
matching mountpoint. It will return the Fruitbak::Share::Read object
together with the remainder of the path.

If no match is found, but one of the shares has a name that is exactly
equal to the requested path, that share is returned instead.

The intention is to make it easy to find a path in the backups without
having to remember the names of the shares or which share contains what.

Note that this function does not check whether the path actually exists,
it just returns the share in which the path is most likely to be found.

=cut

sub resolve_share {
	my $path = shift;
	my @path = split_path($path);

	my ($bestbase, $bestshare, $bestnum);

	my $shares = $self->shares;
	share: foreach my $name (@$shares) {
		my $share = $self->get_share($name);
		if(!defined $bestshare && $name eq $path) {
			$bestshare = $share;
			$bestbase = '';
			$bestnum = -1;
		}
		my @share = split_path($share->mountpoint);
		next if @share > @path;
		next if defined $bestnum && @share <= $bestnum;
		my $num = @share;
		my @base = @path;
		while(@share) {
			my $s = shift @share;
			my $b = shift @base;
			next share if $s ne $b;
		}
		$bestnum = $num;
		$bestbase = join('/', @base);
		$bestshare = $share;
	}
	return $bestshare, $bestbase if defined $bestshare;
	die "no such share '$path'\n";
}

=back

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

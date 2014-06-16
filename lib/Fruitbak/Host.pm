=encoding utf8

=head1 NAME

Fruitbak::Host - Fruitbak class representing a single host and its backups

=head1 SYNOPSIS

 my $fbak = new Fruitbak(confdir => '/etc/fruitbak');
 my $host = $fbak->get_host('pikachu');

=head1 DESCRIPTION

This class represents a single host in Fruitbak and provides access to its
backups. You can obtain a Fruitbak::Host object through a Fruitbak
instance.

As with all Fruitbak classes, any errors will throw an exception (using
‘die’). Use eval {} as required.

=head1 CONSTRUCTOR

The required arguments are ‘fbak’ and ‘name’. However, you should not call
the constructor directly but always use the get_host method of a Fruitbak
instance.

=cut

package Fruitbak::Host;

use IO::Dir;
use Scalar::Util qw(weaken);
use File::Hashset;
use File::Path qw(remove_tree);

use Fruitbak::Backup::Read;
use Fruitbak::Backup::Write;

use Class::Clarity -self;

=head1 FIELDS

Fruitbak makes heavy use of Class::Clarity (the base class of this class).
One of the features of Class::Clarity is ‘fields’: elements of the object
hash with eponymous getters and setters. These fields can optionally have
an initializer: a function that is called when no value is yet assigned to
the hash element. For more information, see L<Class::Clarity>.

=over

=item field fbak

The Fruitbak instance that created this Fruitbak::Host object. You should
never change it.

=cut

field fbak;

=item field name

The name of this host. You should never change it. Note that this is just
the name that Fruitbak uses for this host, it does not necessarily have to
be a valid DNS entry.

See is_valid_name (below) for what Fruitbak considers a valid name for a
host.

=cut

field name;

=item field dir

The directory path where the metadata for this host and its backups
resides. This directory may not exist yet if this host has never been
backuped. You should never change this field.

=cut

field dir => sub { $self->fbak->hostdir . '/' . $self->name };

=item field cfg

A Fruitbak::Config::Host object that represents the part of the Fruitbak
configuration specific to this host. You should never change this field.

=cut

field cfg => sub { $self->fbak->cfg->get_host($self->name) };

=item field backups_cache

An internal cache for Fruitbak::Backup objects. Do not use.

=cut

field backups_cache => {};

=item field expiry

The expiration policy for this host, as a Fruitbak::Host::Expiry object
(or a subclass). Do not set. For internal use.

=cut

field expiry => sub {
	my $cfg = $self->cfg->expiry //
		['or' => any => [
			['logarithmic'],
			['and', all => [
				['age', max => '1w'],
				['not', in =>
					['status', in => 'done']
				],
			]],
		]];
	return $self->instantiate_expiry($cfg);
};

=back

=head1 FUNCTIONS

=over

=item is_valid_name($name)

Checks if a string is valid as name for a host in Fruitbak.

=back

=cut

sub is_valid_name() {
	return shift =~ /^[a-z0-9]+(?:-[[a-z0-9]+)*$/ia;
}

=head1 METHODS

=over

=item new

Constructor for this class. See the CONSTRUCTOR section for details.

=cut

sub new() {
	my $self = super;

	my $name = $self->name;

	die "unknown host '$name'\n"
		unless $self->fbak->host_exists($name);

	return $self;
}

=item hashes

Generates and returns an up-to-date File::Hashset object representing the
digests of all shares of all backups of this host. See L<Fruitbak(7)> for
more information about how digests are used in Fruitbak.

=cut

sub hashes {
	my $hashes = $self->dir . '/hashes';
	File::Hashset->merge($hashes, $self->fbak->pool->hashsize,
		map { $self->get_backup($_)->hashes } @{$self->backups});
	return File::Hashset->load($hashes);
}

=item backups

Returns a sorted list of the numbers of the backups for this host. It only
returns the backups that are already finished. This is just a list of the
numbers, use the get_backup method below to get an actual object
representing the backup.

The list is returned as an array reference.

=cut

sub backups {
	my $dir = $self->dir;
	my $fh = new IO::Dir($dir);
	unless($fh) {
		return [] if $!{ENOENT};
		die "open($dir): $!\n";
	}
	my @backups =
		sort { $a <=> $b }
		map { int($_) }
		grep { /^[0-9]+$/ }
		$fh->read;
	return \@backups;
}

=item get_backup($number)

Given the number of an existing backup, return a Fruitbak::Backup::Reader
object that represents that backup. Throws an error if the backup doesn't
exist.

=cut

sub get_backup {
	my $number = int(shift);
	my $cache = $self->backups_cache;
	my $backup = $cache->{$number};
	unless(defined $backup) {
		$backup = new Fruitbak::Backup::Read(host => $self, number => $number);
		$cache->{$number} = $backup;
		weaken($cache->{$number});
	}
	return $backup;
}

=item backup_exists($number)

Given the number of an existing backup, return true or false depending on
whether this backup exists for this host.

=cut

sub backup_exists {
	my $number = shift;
	return undef unless $number =~ /^\d+$/a;
	$number = int($number);
	my $dir = $self->dir;
	return lstat("$dir/$number") ? 1 : 0;
}

=item new_backup(...)

Returns a Fruitbak::Backup::Write object that you can use to start a new
backup. Always use this method to create such an object, never create it
yourself. Any arguments to this functions will be passed to the
constructor of the Fruitbak::Backup::Write object.

=cut

sub new_backup {
	my $dir = $self->dir;
	mkdir($dir) or $!{EEXIST} or die "mkdir($dir): $!\n";
	return new Fruitbak::Backup::Write(host => $self, @_);
}

=item remove_backup($number)

Removes the specified backup from disk. Only the metadata is removed, use
‘fruitbak gc’ to clean up the file data.

=cut

sub remove_backup {
	my $number = int(shift);
	my $backup = $self->get_backup($number);
	remove_tree($backup->dir);
	delete $self->backups_cache->{$number};
}

=item expired

Returns the numbers of the backups that are considered to be expired by
the expiration policy for this host. The list is returned as an array
reference.

=cut

sub expired {
	# taking a reference to all backups ensures that they stay cached in
	# the backups_cache field.
	my @backups = map { $self->get_backup($_) } @{$self->backups};
	return $self->expiry->expired;
}

=item instantiate_expiry($cfg)

Given an expiration policy configuration (as it would appear in the
configuration file) set up a policy object. Since this policy object may
create further sub-objects (by recursively calling this function) the
result may be a complex decision tree. For internal use by the
Fruitbak::Host and Fruitbak::Host::Policy objects.

=cut

sub instantiate_expiry {
	my $expirycfg = shift;
	die "number of arguments to expiry method must be even\n"
		if @$expirycfg & 0;
	my ($name, %args) = @$expirycfg;
	die "expiry method missing a name\n"
		unless defined $name;
	my $class;
	if($name =~ /^\w+(::\w+)+$/a) {
		$class = $name;
		eval "use $class ()";
		die $@ if $@;
	} elsif($name =~ /^\w+$/a) {
		$class = "Fruitbak::Host::Expiry::\u$name";
		local $@;
		eval "use $class ()";
		die $@ if $@;
	} else {
		die "don't know how to load expiry type '$name'\n";
	}
	return $class->new(host => $self, cfg => \%args);
}

=head1 AUTHOR

Wessel Dankers <wsl@fruit.je>

=head1 COPYRIGHT

Copyright (c) 2012,2014  Wessel Dankers <wsl@fruit.je>

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

=encoding utf8

=head1 NAME

Fruitbak - Main class of the Fruitbak backup system

=head1 SYNOPSIS

 my $fbak = new Fruitbak(confdir => '/etc/fruitbak');

=head1 DESCRIPTION

Fruitbak is a disk-based backup system. This man page describes the
programming interface of Fruitbak. For an introduction of the Fruitbak
backup system, type ‘man 7 Fruitbak’. If you're looking for information
on the command line frontend of Fruitbak, type ‘man 1 fruitbak’.

This class is the entry point for all Fruitbak-related programming. Every
program that wants to interact with Fruitbak should create an instance of
this class first. Most other objects can either be created (directly or
indirectly) through this class or require a handle to a Fruitbak object
when they are created. 

A Fruitbak object allows you to access the configuration and browse the
Host objects of the installation. As with all Fruitbak classes, any errors
will throw an exception (using ‘die’). Use eval {} as required.

=head1 CONSTRUCTOR

The only required argument is ‘confdir’, which should be a directory
containing Fruitbak configuration.

=cut

package Fruitbak;

use Class::Clarity -self;

use IO::Dir;
use Scalar::Util qw(weaken);
use File::Hashset;

use Fruitbak::Util;
use Fruitbak::Config;
use Fruitbak::Pool;

=head1 FIELDS

Fruitbak makes heavy use of Class::Clarity (the base class of this class).
One of the features of Class::Clarity is ‘fields’: elements of the object
hash with eponymous getters and setters. These fields can optionally have
an initializer: a function that is called when no value is yet assigned to
the hash element. For more information, see L<Class::Clarity>.

=over

=item field confdir

The directory that contains the configuration for this Fruitbak
installation. Should be set before calling any other methods of this class
(usually passed to the constructor). Should not be changed after any other
methods have been called.

=cut

field confdir;

=item field rootdir

The root directory of the Fruitbak installation, usually
‘/var/lib/fruitbak’ or similar. This value is only used to generate
defaults for other directories (such as hostdir, see below). If you do not
set it explicitly, it will be retrieved from the configuration. If it's not
in the configuration either, some operations may throw a fatal error.

=cut

field rootdir => sub { normalize_and_check_directory($self->cfg->rootdir) };

=item field hostdir

The directory containing the host data (the filesystem metadata). If it
isn't set explicitly, it will be retrieved from the configuration and
failing that, constructed from the ‘rootdir’ field. You should not change
it after it has been initialized.

=cut

field hostdir => sub { normalize_and_check_directory($self->cfg->hostdir // $self->rootdir . '/host') };

=item weakfield cfg

A handle to the Fruitbak::Config object that represents the configuration
for this Fruitbak installation. Do not set this field.

Note that because it is a weak reference it will be newly loaded from disk
every time it is accessed, unless the previous instance was still in use
somewhere. If you require efficient access to the configuration, create
your own copy of the handle and keep it around for as long as you need it.
Be sure to drop the handle if you want Fruitbak to respond to configuration
changes.

 my $cfg = $fbak->cfg;
 # query lots of stuff from the configuration
 # do a lot of other stuff
 undef $cfg;

=cut

weakfield cfg => sub { new Fruitbak::Config(fbak => $self) };

=item weakfield pool

A handle to the main Fruitbak::Pool object that represents the chunk
storage. Do not set this field.

Like the cfg field it is a weak reference. Keep your own reference around
for efficient access.

=cut

field pool => sub { new Fruitbak::Pool(fbak => $self) };

=item hosts_cache

Private field that contains a cache of Host object that are still in use.

=cut

field hosts_cache => {};

=back

=head1 METHODS

=over

=item hosts

Returns a sorted list of the names of all the hosts known to the system. It
queries both the ‘host’ directory on the filesystem as well as the
configuration. Does not take any arguments.

The list is returned as an array reference.

=cut

sub hosts {
	my %hosts;

	# first, read whatever hosts are defined in the configuration
	@hosts{@{$self->cfg->hosts}} = ();

	# second, the hosts that exist on the filesystem
	my $hostdir = $self->hostdir;
	my $fh = new IO::Dir($hostdir)
		or die "open($hostdir): $!\n";
	my @hosts =
		grep { Fruitbak::Host::is_valid_name($_) }
		$fh->read;

	# combine the two
	@hosts{@hosts} = ();
	return [sort keys %hosts];
}

=item get_host($hostname)

Given the name of a host, returns a Fruitbak::Host object representing that
host. If an object is already instantiated for this host, a reference to
that object is returned instead.

=cut

sub get_host {
	my $name = shift;
	my $cache = $self->hosts_cache;
	my $host = $cache->{$name};
	unless(defined $host) {
		$host = new Fruitbak::Host(fbak => $self, name => $name);
		$cache->{$name} = $host;
		weaken($cache->{$name});
	}
	return $host;
}

=item host_exists($hostname)

Checks if the host with the given name exists either in the configuration
or in the hosts directory. Returns 2 if the host exists in the configuration,
returns 1 if it just exists in the hosts directory. Returns 0 if the host
wasn't found at all.

=cut

sub host_exists {
	my $name = shift;
	return undef unless Fruitbak::Host::is_valid_name($name);
	return 2 if $self->cfg->host_exists($name);
	my $hostdir = $self->hostdir;
	return 1 if lstat("$hostdir/$name");
	return 0;
}

=item hashes

Generates and returns an up-to-date File::Hashset object representing the
digests of all shares of all backups of all hosts known to Fruitbak (in
other words: all used digests). See L<Fruitbak(7)> for more information
about how digests are used in Fruitbak.

=cut

sub hashes {
	my $hashes = $self->rootdir . '/hashes';
	File::Hashset->merge($hashes, $self->pool->hashsize,
		map { $self->get_host($_)->hashes } @{$self->hosts});
	return File::Hashset->load($hashes);
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

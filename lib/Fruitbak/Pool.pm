=encoding utf8

=head1 NAME

Fruitbak::Pool - Fruitbak class that represents and provides access to the pool subsystem

=head1 SYNOPSIS

 my $fbak = new Fruitbak(confdir => '/etc/fruitbak');
 my $pool = $fbak->pool;

=head1 DESCRIPTION

This class represents the pool subsystem in Fruitbak and provides access to
its data. You can obtain a Fruitbak::Pool object through a Fruitbak
instance.

The pool subsystem is responsible for storing the file contents of files
that are backed up using Fruitbak. It does not store metadata or non-file
entries such as directories or symlinks.

File contents are split into chunks and addressed by its digest (default
SHA256). This ensures that data is deduplicated. Files smaller than the
configured chunk size are not split up. If a file is larger than the
configured chunk size, it will have one or more chunks of exactly the
chunk size and only the last part will be a smaller chunk (unless the
file size happened to be an exact multiple of the chunk size).

The actual chunk storage is handled by Fruitbak::Storage objects, which
can be configured into a tree structure to provide features such as
compression, encryption and access to cloud data.

This class provides methods to store, retrieve and remove such chunks.
For an easy way to split and reassemble file contents, see the
Fruitbak::Pool::Read and Fruitbak::Pool::Write classes.

As with all Fruitbak classes, any errors will throw an exception (using
‘die’). Use eval {} as required.

=head1 CONSTRUCTOR

The only required argument is ‘fbak’. However, you should not call the
constructor directly but always use the pool method of a Fruitbak instance.

=cut

package Fruitbak::Pool;

use Class::Clarity -self;

use Digest::SHA;
use MIME::Base64;

use Fruitbak::Pool::Read;
use Fruitbak::Pool::Write;
use Fruitbak::Util;

=head1 FIELDS

Fruitbak makes heavy use of Class::Clarity (the base class of this class).
One of the features of Class::Clarity is ‘fields’: elements of the object
hash with eponymous getters and setters. These fields can optionally have
an initializer: a function that is called when no value is yet assigned to
the hash element. For more information, see L<Class::Clarity>.

=over

=item weakfield fbak

The Fruitbak instance that created this Fruitbak::Pool object. You should
never change it.

=cut

weakfield fbak;

=item field cfg

The Fruitbak::Config object that represents the global Fruitbak
configuration. Do not set.

=cut

field cfg => sub { $self->fbak->cfg };

=item field hashalgo

The hash algorithm used to identify chunks. This is stored as a code ref.
Can be configured through the configuration file. Do not set.

=cut

field hashalgo => sub { $self->cfg->hashalgo // \&Digest::SHA::sha256 };

=item field hashsize

The size of the hashes that the above hashalgo produces. Calculated
automatically. Do not set.

=cut

field hashsize => sub { length($self->hashalgo->('')) };

=item field chunksize

The size of the chunks in which files are split up.

=cut

field chunksize => sub { $self->cfg->chunksize // 2097152 };

field storage => sub {
	my $cfg = $self->cfg;
	my $storagecfg = $cfg->pool // ['filesystem'];
	return $self->instantiate_storage($storagecfg);
};

sub instantiate_storage {
	my $storagecfg = shift;
	die "number of arguments to storage method must be even\n"
		if @$storagecfg & 0;
	my ($name, %args) = @$storagecfg;
	die "storage method missing a name\n"
		unless defined $name;
	my $class;
	if($name =~ /^\w+(::\w+)+$/a) {
		$class = $name;
		eval "use $class ()";
		die $@ if $@;
	} elsif($name =~ /^\w+$/a) {
		$class = "Fruitbak::Storage::\u$name";
		local $@;
		eval "use $class ()";
		die $@ if $@;
	} else {
		die "don't know how to load storage type '$name'\n";
	}
	return $class->new(pool => $self, cfg => \%args);
}

sub store {
	$self->storage->store(@_);
	return;
}

sub retrieve {
	return $self->storage->retrieve(shift);
}

sub exists {
	return $self->storage->exists(shift);
}

sub remove {
	$self->storage->remove(shift);
	return;
}

sub iterator {
	return $self->storage->iterator(@_);
}

sub reader {
	return new Fruitbak::Pool::Read(pool => $self, @_);
}

sub writer {
	return new Fruitbak::Pool::Write(pool => $self, @_);
}

# for debugging
sub digestlist {
	my $hashsize = $self->hashsize;
	my @hashes = map { encode_base64($_, '')."\n" } unpack("(a$hashsize)*", shift);
	return join('', @hashes);
}

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

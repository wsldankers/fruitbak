=encoding utf8

=head1 NAME

Fruitbak::Pool - represents and provides access to the pool subsystem

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

package Fruitbak::Pool;

use Class::Clarity -self;

use Digest::SHA;
use MIME::Base64;

use Fruitbak::Pool::Read;
use Fruitbak::Pool::Write;
use Fruitbak::Util;

weakfield fbak;
field cfg => sub { $self->fbak->cfg };
field hashalgo => sub { \&Digest::SHA::sha256 };
field hashsize => sub { length($self->hashalgo->('')) };
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
		foreach($name, uc($name), ucfirst($name), ucfirst(lc($name))) {
			my $classname = "Fruitbak::Pool::Storage::$_";
			if($classname->can('has') || eval "use $classname (); 1") {
				$class = $classname;
				last;
			}
		}
		die $@ unless defined $class;
	} else {
		die "don't know how to load storage type '$name'\n";
	}
	return $class->new(pool => $self, cfg => \%args);
}

sub store {
	my $data = shift;
	my $hash = $self->hashalgo->($$data);
	$self->storage->store($hash, $data);
	return $hash;
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

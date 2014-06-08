=encoding utf8

=head1 NAME

Fruitbak::Host - class for backup related bookkeeping of each host

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

package Fruitbak::Host;

use IO::Dir;
use Scalar::Util qw(weaken);
use File::Hashset;
use File::Path qw(remove_tree);
use Fruitbak::Backup::Read;
use Fruitbak::Backup::Write;

use Class::Clarity -self;

field fbak; # (Fruitbak::Lib) required for new
field dir => sub { $self->fbak->hostdir . '/' . $self->name };
field name; # (string) required for new
field create_ok => undef; # (bool) whether the host can be created if it doesn't exist
field backups_cache => {};
field cfg => sub { $self->fbak->cfg->get_host($self->name) };

sub is_valid_name() {
	return shift =~ /^[a-z0-9]+(?:-[[a-z0-9]+)*$/ia;
}

sub new() {
	my $self = super;

	my $name = $self->name // 'UNDEF';
	die "'$name' is not a valid host name\n"
		unless is_valid_name($name);

	my $dir = $self->dir;
	unless(-d $dir) {
		if($self->create_ok) {
			mkdir($dir) or die "mkdir($dir): $!\n";
		} else {
			die "'$dir' does not exist or is not a directory\n";
		}
	}

	return $self;
}

field hashes => sub {
	my $hashes = $self->dir . '/hashes';
	File::Hashset->merge($hashes, $self->fbak->pool->hashsize,
		map { $self->get_backup($_)->hashes } @{$self->backups});
	return File::Hashset->load($hashes);
};

# return a sorted list of backups for this host
field backups => sub {
	my $dir = $self->dir;
	my $fh = new IO::Dir($dir)
		or die "open($dir): $!\n";
	my @backups =
		sort { $a <=> $b }
		map { int($_) }
		grep { /^[0-9]+$/ }
		$fh->read;
	return \@backups;
};

# given a number, return a Fruitbak::Backup::Read object
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

sub new_backup {
	return new Fruitbak::Backup::Write(host => $self, @_);
}

sub remove_backup {
	my $number = int(shift);
	my $backup = $self->get_backup($number);
	remove_tree($backup->dir);
	delete $self->backups_cache->{$number};
}

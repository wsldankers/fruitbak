=encoding utf8

=head1 NAME

Fruitbak::Backup::Read - access a specific backup of a specific host

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

package Fruitbak::Backup::Read;

use Class::Clarity -self;

use IO::File;
use IO::Dir;
use JSON;
use Scalar::Util qw(weaken);
use Fruitbak::Share::Read;
use Fruitbak::Share::Format;

field fbak => sub { $self->host->fbak };
field host; # required for new
field number; # required for new
field dir => sub { $self->host->dir . '/' . $self->number };
field sharedir => sub { $self->dir . '/share' };
field shares_cache => {};
field compress => sub { $self->fbak->compress };

field info => sub {
	my $dir = $self->dir;
	my $info = new IO::File("$dir/info.json", '<')
		or die "open($dir/info.json): $!\n";
	my $json = do { local $/; <$info> };
	$info->eof or die "read($dir/info.json): $!\n";
	$info->close;
	return decode_json($json);
};
field level => sub { $self->info->{level} };
field type => sub { $self->level ? 'incr' : 'full' };
field startTime => sub { $self->info->{startTime} };
field endTime => sub { $self->info->{endTime} };

# return a list of names, one for each share in this backup
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

# given a name, return a Fruitbak::Share::Read object
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

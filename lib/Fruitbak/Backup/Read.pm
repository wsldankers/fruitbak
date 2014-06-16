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
use File::Hashset;
use Fruitbak::Util;
use Fruitbak::Share::Read;
use Fruitbak::Share::Format;

field fbak => sub { $self->host->fbak };
field host; # required for new
field number; # required for new
field dir => sub { $self->host->dir . '/' . $self->number };
field sharedir => sub { $self->dir . '/share' };
field shares_cache => {};

field info => sub {
	my $dir = $self->dir;
	my $info = new IO::File("$dir/info.json", '<')
		or die "open($dir/info.json): $!\n";
	my $json = do { local $/; <$info> };
	$info->eof or die "read($dir/info.json): $!\n";
	$info->close;
	return decode_json($json);
};
field status => sub { $self->info->{status} };
field level => sub { $self->info->{level} };
field full => sub { !$self->level };
field startTime => sub { $self->info->{startTime} };
field endTime => sub { $self->info->{endTime} };

field hashes => sub {
	my $hashes = $self->dir . '/hashes';
	unless(-e $hashes) {
		File::Hashset->merge($hashes, $self->fbak->pool->hashsize,
			map { $self->get_share($_)->hashes } @{$self->shares});
	}
	return File::Hashset->load($hashes);
};

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
		my @share = split_path($share->path);
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

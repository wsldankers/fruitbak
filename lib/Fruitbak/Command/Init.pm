=encoding utf8

=head1 NAME

Fruitbak::Command::Init - implementation of CLI init command

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

package Fruitbak::Command::Init;

use autodie;
use IO::File;
use File::Path qw(make_path);
use Cwd qw(getcwd);
use Fruitbak::Util;

use Fruitbak::Command -self;

BEGIN {
	$Fruitbak::Command::commands{init} = [__PACKAGE__, "Initialize a fruitbak environment"];
}

sub writefile {
	my ($name, $contents) = @_;
	my $fh = new IO::File($name, '>')
		or die "open($name): $!\n";
	$fh->write($contents)
		or die "write($name): $!\n";
	$fh->flush
		or die "write($name): $!\n";
	$fh->sync
		or die "fsync($name): $!\n";
	$fh->close
		or die "write($name): $!\n";
}

sub run {
	my (undef, $dir) = @_;
	if(defined $dir) {
		$dir = getcwd()."/$dir"
			unless $dir =~ m{^/};
	} else {
		$dir = getcwd();
	}
	make_path($dir, "$dir/conf/host", "$dir/host", "$dir/pool", "$dir/cpool");
	$dir = normalize_and_check_directory($dir);
	my $escapeddir = $dir =~ s/([\\"])/\\$1/gr;
	$self->writefile("$dir/conf/global.pl", <<EOT);
our %conf;
\$conf{rootdir} = "$escapeddir";
EOT
	$self->writefile("$dir/lock", '');
	return 0;
}

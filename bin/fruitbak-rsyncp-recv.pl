#! /usr/bin/perl

=encoding utf8

=head1 NAME

fruitbak-rsyncp-recv - process wrapper for File::RsyncP

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

use strict;
use warnings FATAL => 'all';

use File::Temp;
use File::RsyncP;
use Fruitbak::Transfer::Rsync::IO;

binmode STDIN;
binmode STDOUT;

$SIG{PIPE} = 'IGNORE';

my $lock = new File::Temp(EXLOCK => 0);

open(my $in, '<&', \*STDIN)
	or die "dup(STDIN): $!\n";
open(my $out, '>&', \*STDOUT)
	or die "dup(STDOUT): $!\n";
open(STDIN, '<', '/dev/null')
	or die "open(/dev/null): $!\n";
open(STDOUT, '>&', \*STDERR)
	or die "dup2(STDOUT, STDERR): $!\n";

my $fio = new Fruitbak::Transfer::Rsync::IO(
	in => $in,
	out => $out,
	lockfh => $lock,
	lockpid => $$,
	lockname => $lock->filename,
);

my $command = shift @ARGV;
my $path = shift @ARGV;

my $rs = new File::RsyncP({
#	logLevel => 9001,
	rsyncCmd => ['/bin/sh', '-ec', $command, 'sh'],
	rsyncArgs => \@ARGV,
	fio => $fio,
});

eval {
	$rs->remoteStart(1, $path);
	$rs->go('/DUMMY');
	$rs->serverClose;
};
if(my $err = $@) {
	eval { $rs->abort };
	warn $@ if $@;
	die $err;
}
exit 0;

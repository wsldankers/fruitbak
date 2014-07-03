#! /usr/bin/perl

use strict;
use warnings FATAL => 'all';

our $pkglocalstatedir //= '.';
our $pkgsysconfdir //= 'conf';
our $pkgdatadir;

use Fruitbak;
use Fruitbak::Command;

local $ENV{PATH} = "$pkgdatadir/bin:$PATH"
	if defined $pkgdatadir;

my $fbak = exists $ENV{FRUITBAK}
	? new Fruitbak(rootdir => $ENV{FRUITBAK})
	: new Fruitbak(rootdir => $pkglocalstatedir, confdir => $pkgsysconfdir);

my $cmd = new Fruitbak::Command(fbak => $fbak);

exit $cmd->run(@ARGV);

=pod

=encoding utf8

=head1 NAME

fruitbak - command line frontend to the Fruitbak backup system

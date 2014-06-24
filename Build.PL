#! /usr/bin/env perl

use strict;
use warnings FATAL => 'all';

use Module::Build;

use File::Find;

do {
	open my $fh, '>', 'MANIFEST'
		or die "open(MANIFEST): $!\n";
	find({ wanted => sub { print $fh "$_\n" or die "write(MANIFEST): $!\n" }, no_chdir => 1, follow => 0}, 'lib');
	close $fh or die "close(MANIFEST): $!\n";
};

my $build = new Module::Build(
	dist_name => 'hello-perl',
	dist_version => '1.0',
	dist_abstract => 'Demo for perl packaging',
	dist_author => 'Wessel Dankers <wsl@fruit.je>',
	module_name => 'Hello',
	license => 'perl',
	script_files => [],
	create_makefile_pl => 'small',
);
$build->create_build_script;

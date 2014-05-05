#! /usr/bin/perl

use strict;
use warnings FATAL => 'all';

use Test::More;
use Data::Dumper;
use Carp qw(confess);
use MIME::Base64;
use Fcntl qw(:mode);

BEGIN { use_ok('Fruitbak') or BAIL_OUT('need Fruitbak to run') }

my $testdir = '/tmp/fruitbak-test-config';
sub cleantestdir {
	local $ENV{testdir} = $testdir;
	system('rm -rf -- "$testdir"');
}
sub mkdir_or_die {
	local $_;
	mkdir $_ or die "mkdir($_): $!\n"
		foreach @_;
}
cleantestdir();
#END { cleantestdir() }
mkdir_or_die($testdir, "$testdir/host", "$testdir/conf", "$testdir/conf/host", "$testdir/pool", "$testdir/cpool");

sub writefile {
	my ($name, $contents) = @_;
	open my $fh, '>', $name
		or die "open($name): $!\n";
	print $fh $contents
		or die "write($name): $!\n";
	close $fh
		or die "write($name): $!\n";
}

writefile("$testdir/conf/config.pl", <<EOT);
our %conf;
\$conf{rootdir} = '$testdir';
\$conf{concurrent_jobs} = 42;
EOT

my $fbak = new_ok('Fruitbak', [confdir => "$testdir/conf"])
	or BAIL_OUT('need a Fruitbak object to run');

$fbak->hosts;

$fbak->get_host('pikachu', create_ok => 1);

done_testing();

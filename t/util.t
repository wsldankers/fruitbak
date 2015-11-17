#! /usr/bin/perl

use strict;
use warnings FATAL => 'all';

use Test::More;
use Data::Dumper;
use Carp qw(confess);
use MIME::Base64;
use Fcntl qw(:mode);

BEGIN { use_ok('Fruitbak::Util') or BAIL_OUT('need Fruitbak::Util to run') }

foreach(
	['//' => '/'],
	['/.' => '/'],
	['/./' => '/'],
	['//.//' => '/'],
	['.//' => '.'],
	['.' => '.'],
	['' => ''],
	['/././././.' => '/'],
	['./././././.' => '.'],
	['././././././' => '.'],
	['././././././' => '.'],
	['./././foo/./././' => 'foo'],
	['foo/' => 'foo'],
) {
	my ($test, $expect) = @$_;
	is(Fruitbak::Util::normalize_path($test), $expect, "normalize '$test'");
}

done_testing();

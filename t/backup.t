#! /usr/bin/perl

use strict;
use warnings FATAL => 'all';

$SIG{__DIE__} = sub {
	local $_ = join('', @_);
#	if(s/^.*\K at \S+ line \d+\.?\n\z//) {
		local $Carp::CarpLevel = 1;
		confess($_);
#	} else {
#		die $_;
#	}
};

use Test::More;
use Data::Dumper;
use Carp qw(confess);
use MIME::Base64;
use Fcntl qw(:mode);

BEGIN { use_ok('Fruitbak') or BAIL_OUT('need Fruitbak to run') }
BEGIN { use_ok('Fruitbak::Backup::Write') or BAIL_OUT('need Fruitbak::Backup::Write to run') }
BEGIN { use_ok('Fruitbak::Share::Write') or BAIL_OUT('need Fruitbak::Share::Write to run') }
BEGIN { use_ok('Fruitbak::Dentry') or BAIL_OUT('need Fruitbak::Dentry to run') }

my $testdir = '/tmp/fruitbak-test-backup';
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

writefile("$testdir/conf/global.pl", <<EOT);
our %conf;
\$conf{rootdir} = '$testdir';
\$conf{concurrent_jobs} = 42;
EOT

my $fbak = new_ok('Fruitbak', [confdir => "$testdir/conf"])
	or BAIL_OUT('need a Fruitbak object to run');

my $ho = $fbak->get_host('test', create_ok => 1);

my $refdata = 'herpderp!'x7e6;

do {
	my $bu = new_ok('Fruitbak::Backup::Write', [host => $ho, number => 0])
		or BAIL_OUT('need a Fruitbak::Backup::Write object to run');
	my $shw = new_ok('Fruitbak::Share::Write', [backup => $bu, name => '/', compress => 1])
		or BAIL_OUT('need a Fruitbak::Share::Write object to run');

	$shw->add_entry(new Fruitbak::Dentry(
			name => 'foo',
			size => 123,
			mtime => 1234567890,
			mode => S_IFDIR | 0755,
			uid => 1000,
			gid => 1000,
	));

	$shw->add_entry(new Fruitbak::Dentry(
			name => 'linktarget',
			size => 123,
			mtime => 1234567890,
			mode => S_IFLNK | 0777,
			uid => 1000,
			gid => 1000,
			symlink => 'derp',
			hardlink_target => 1,
	));

	$shw->add_entry(new Fruitbak::Dentry(
			name => 'hardlink',
			size => 123,
			mtime => 1234567890,
			mode => S_IFLNK | 0777,
			uid => 1000,
			gid => 1000,
			hardlink => 'linktarget',
	));

	my $file = $fbak->pool->writer;
	$file->write($refdata);
	my ($hashes, $size) = $file->close;

	$shw->add_entry(new Fruitbak::Dentry(
			name => 'foo/bar',
			size => $size,
			mtime => 1234567890,
			mode => S_IFDIR | 0755,
			uid => 1000,
			gid => 100,
			extra => $hashes,
	));

	$shw->finish;
	$bu->finish;
};

do {
	my $baks = $ho->backups;
	is_deeply($baks, [0], "host has exactly one backup with number 0");
	my $bak = $ho->get_backup(0);
	my $shares = $bak->shares;
	is_deeply($shares, ['/'], "backup has exactly one share with name '/'");
	my $share = $bak->get_share('/');
	is($share, $bak->get_share('/'), "retrieving the same share twice yields the same object");

	is_deeply($share->ls('foo'), ['foo/bar']);
	is(eval { $share->get_entry('hardlink')->symlink }, undef);
	is($share->get_entry('hardlink', 1)->symlink, 'derp');
	is($share->get_entry('hardlink')->hardlink, 'linktarget');
	is($share->get_entry('hardlink', 1)->hardlink, undef);

	ok($share->hh->exists('foo'), "share contains foo");
	my $entry = $share->get_entry('foo/bar');
	ok($entry, "able to retrieve foo entry");
	is($entry->name, 'foo/bar', "entry name matches");
	is($entry->size, length($refdata), "entry size matches");
	is($entry->mtime, 1234567890, "entry mtime matches");
	is($entry->mode, S_IFDIR | 0755, "entry mode matches");
	is($entry->uid, 1000, "entry uid matches");
	is($entry->gid, 100, "entry gid matches");

	my $pr = $fbak->pool->reader(digests => $entry->extra);
	my $testdata = $pr->read(length($refdata) + 1);
	is(length($testdata), length($refdata), "length of read back data equals original")
		and is($testdata, $refdata, "read back data equals original");
};

done_testing();

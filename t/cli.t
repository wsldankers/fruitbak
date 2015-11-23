#! /usr/bin/perl

use strict;
use warnings FATAL => 'all';

use Test::More;
use File::Temp;
use File::Path qw(remove_tree);
use POSIX qw(_exit :sys_wait_h);
use IPC::Open3;
use Cwd;

use Data::Dumper;
use Carp qw(confess);

sub mkdir_or_die {
	local $_;
	mkdir $_ or die "mkdir($_): $!\n"
		foreach @_;
}

my $cwd = getcwd();
my $testdir = File::Temp->newdir;

#my $testdir = '/tmp/fruitbak-test-backup';
#mkdir_or_die($testdir);
#remove_tree($testdir, {keep_root => 1});

$ENV{FRUITBAK} = $testdir;
$ENV{TZ} = 'UTC';
delete @ENV{grep { /^(?:LANG|LC_)/ } keys %ENV};

mkdir_or_die("$testdir/bin");
foreach my $exe (<bin/*.pl>) {
	my $dest = "$testdir/$exe" =~ s/\.pl\z//ra;
	symlink("$cwd/$exe", $dest)
		or die "symlink($cwd/$exe, $dest): $!\n";
}
$ENV{PATH} = "$testdir/bin:$ENV{PATH}";

open my $devnull, '<', '/dev/null'
	or die "open(/dev/null): $!\n";

sub run {
	my $prog = join(' ', @_);
	my $pid = open3($devnull, my $out, undef, '-');
	die "open3(): $!\n" unless defined $pid;
	unless($pid) {
		exec { $_[0] } @_;
		warn $!;
		_exit(2);
	}
	my $output = do { local $/; <$out> } // '';
	$output =~ s/[^\n]\K\z/\n/a;
	waitpid($pid, 0) or die "waitpid($pid): $!\n";
	if(WIFEXITED($?)) {
		my $status = WEXITSTATUS($?);
		return sprintf("%s%s exited with status %d\n", $output, $prog, $status)
			if $status;
	} elsif(WIFSIGNALED($?)) {
		my $sig = WTERMSIG($?);
		return sprintf("%s%s killed with signal %d%s\n", $output, $prog, $sig & 127, ($sig & 128) ? ' (core dumped)' : '');
	} elsif(WIFSTOPPED($?)) {
		my $sig = WSTOPSIG($?);
		return sprintf("%s%s stopped with signal %d\n", $output, $prog, $sig);
	}
	return $output;
}

sub writefile {
	my ($name, $contents) = @_;
	open my $fh, '>', $name
		or die "open($name): $!\n";
	print $fh $contents
		or die "write($name): $!\n";
	close $fh
		or die "write($name): $!\n";
}

is(run(qw(fruitbak init)), '');
is(run(qw(fruitbak ls)), "Host name  Last backup  Index  Type  Level  Status\n");

writefile("$testdir/conf/global.pl", <<EOT);
our %conf;
\$conf{concurrent_jobs} = 42;
1;
EOT

writefile("$testdir/conf/common.pl", <<EOT);
our %conf;
\$conf{concurrent_jobs} = 42;
\$conf{exclude} = [qw(/var/excl1 excl2 /usr/incl1 /incl1 /var/incl incl)];
\$conf{shares} = [{name => 'var', mountpoint => '/var', path => "$testdir/source", exclude => [qw(/var/excl3 excl4 /opt/incl1 /incl1)]}];
1;
EOT

writefile("$testdir/conf/host/local.pl", <<EOT);
our %conf;
\$conf{share} = {transfer => ['local']};
1;
EOT

writefile("$testdir/conf/host/rsync.pl", <<EOT);
our %conf;
\$conf{share} = {transfer => ['rsync', command => 'exec rsync "\$@"']};
1;
EOT

mkdir_or_die(
	"$testdir/source",
	"$testdir/source/incl1",
	"$testdir/source/excl1",
	"$testdir/source/excl2",
	"$testdir/source/excl3",
	"$testdir/source/excl4",
);
writefile("$testdir/source/incl1/file.txt", "Hello world!\n");
utime(1234567890, 1234567890, "$testdir/source/incl1/file.txt");
writefile("$testdir/source/excl1/file.txt", "excluded 1\n");
writefile("$testdir/source/excl2/file.txt", "excluded 2\n");
writefile("$testdir/source/excl3/file.txt", "excluded 3\n");
writefile("$testdir/source/excl4/file.txt", "excluded 4\n");

is(run(qw(fruitbak bu)), '');
like(run(qw(fruitbak ls)), qr{^(?:
Host\ name\ +Last\ backup\ +Index\ +Type\ +Level\ +Status\n
local\ +\d{4}-\d{2}-\d{2}\ \d{2}:\d{2}:\d{2}\ +0\ +full\ +0\ +done\n
rsync\ +\d{4}-\d{2}-\d{2}\ \d{2}:\d{2}:\d{2}\ +0\ +full\ +0\ +done\n
)\z}xa);

is(run(qw(fruitbak bu)), '');
like(run(qw(fruitbak ls)), qr{^(?:
Host\ name\ +Last\ backup\ +Index\ +Type\ +Level\ +Status\n
local\ +\d{4}-\d{2}-\d{2}\ \d{2}:\d{2}:\d{2}\ +1\ +incr\ +1\ +done\n
rsync\ +\d{4}-\d{2}-\d{2}\ \d{2}:\d{2}:\d{2}\ +1\ +incr\ +1\ +done\n
)\z}xa);

is(run(qw(fruitbak bu --full)), '');
like(run(qw(fruitbak ls)), qr{^(?:
Host\ name\ +Last\ backup\ +Index\ +Type\ +Level\ +Status\n
local\ +\d{4}-\d{2}-\d{2}\ \d{2}:\d{2}:\d{2}\ +2\ +full\ +0\ +done\n
rsync\ +\d{4}-\d{2}-\d{2}\ \d{2}:\d{2}:\d{2}\ +2\ +full\ +0\ +done\n
)\z}xa);

is(run(qw(fruitbak cat local 0 var incl1/file.txt)), "Hello world!\n");
like(run(qw(fruitbak ls local 0 var incl1)), qr{^\d+  +drwxrwxr-x  +\d+  +\d+  +\d+  +...................  +\.\n\d+  +-rw-rw-r--  +\d+  +\d+  +13  +2009-02-13 23:31:30  +file.txt\n\z}a);

like(run(qw(fruitbak ls local 0 var), ''), qr{^\d+  +drwxrwxr-x  +\d+  +\d+  +\d+  +...................  +\.\n\d+  +drwxrwxr-x  +\d+  +\d+  +\d+  +...................  +incl1\n\z}a);
like(run(qw(fruitbak ls rsync 0 var), ''), qr{^\d+  +drwxrwxr-x  +\d+  +\d+  +\d+  +...................  +\.\n\d+  +drwxrwxr-x  +\d+  +\d+  +\d+  +...................  +incl1\n\z}a);

writefile("$testdir/source/incl1/file.txt", "Hello world?\n");
utime(1234567890, 1234567890, "$testdir/source/incl1/file.txt");

is(run(qw(fruitbak bu)), '');
is(run(qw(fruitbak cat local 3 var incl1/file.txt)), "Hello world!\n");
is(run(qw(fruitbak bu --full)), ''); # should issue a warning
is(run(qw(fruitbak cat local 4 var incl1/file.txt)), "Hello world?\n");
like(run(qw(fruitbak ls local 4 var incl1)), qr{^\d+  +drwxrwxr-x  +\d+  +\d+  +\d+  +...................  +\.\n\d+  +-rw-rw-r--  +\d+  +\d+  +13  +2009-02-13 23:31:30  +file.txt\n\z}a);

done_testing();
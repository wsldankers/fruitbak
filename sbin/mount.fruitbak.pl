#! /usr/bin/perl

use strict;
use warnings FATAL => 'all';

our $pkglocalstatedir //= '.';
our $pkgsysconfdir //= 'conf';
our $pkgdatadir;

use Fuse;
use Data::Dumper;
use Fcntl qw(:mode);
use POSIX qw(_exit setsid O_RDONLY O_ACCMODE);
use Errno qw(ENOENT EROFS EISDIR EINVAL ENOSYS);
use Getopt::Long qw(GetOptions);
use Fruitbak;

GetOptions('o=s' => \my $options) or die "invalid command line\n";

# This is a little quirky: we split the string not on commas but
# on everything *except* the commas. This is necessary because we need
# escape sequence parsing and can't just match any comma.
my @options = split(/(\w+(?:=(?:[^,\\]|\\.)*)?)/a, $options, -1);

die "unable to parse -o argument\n" if (shift @options) !~ /^,*$/a;
die "unable to parse -o argument\n" if (pop @options) !~ /^,*$/a;

my %options;
foreach my $option (@options) {
	next if $option =~ /^,*$/a;
	my $name = $option;
	$name =~ s/=(.*)$//a;
	my $value = $1;
	$options{$name} = $value;
}

die "$0: [-o options] <host>:<backupnum>:<sharename> /directory\n"
	unless @ARGV == 2;

my $fruitbak = $options{fruitbak} // $ENV{FRUITBAK};

my $sharename = $ARGV[0];
$sharename =~ s/^([^:]+)://a
	or die "invalid host/backup/share spec '$ARGV[0]'\n";
my $hostname = $1;
$sharename =~ s/^(\d+)://a
	or die "invalid host/backup/share spec '$ARGV[0]'\n";
my $backupnum = int($1);

my $fbak = defined $fruitbak
	? new Fruitbak(rootdir => $fruitbak)
	: new Fruitbak(rootdir => $pkglocalstatedir, confdir => $pkgsysconfdir);
my $pool = $fbak->pool;
my $host = $fbak->get_host($hostname);
my $backup = $host->get_backup($backupnum);
my $share = $backup->get_share($sharename);
$share->error;

sub dentry2stat {
	my $dentry = shift;
	use integer;
	my $mtime_ns = $dentry->mtime_ns;
	my $mtime = [$mtime_ns / 1e9, $mtime_ns % 1e9];
	my $size = $dentry->size;
	my $blocksize = 4096;
	return
		0, # dev
		$dentry->inode + 1, # ino
		$dentry->mode,
		3, # nlinks
		$dentry->uid,
		$dentry->gid,
		$dentry->is_device ? $dentry->rdev : 0,
		$size,
		$mtime, # atime
		$mtime, # mtime
		$mtime, # ctime
		$blocksize,
		($size + $blocksize - 1) / $blocksize; # blocks
}

Fuse::main(
	mountpoint => $ARGV[1],
	mountopts => 'allow_other,use_ino,ro',
	nullpath_ok => 1,
	nopath => 1,
	init => sub {
		setsid or die "setsid(): $!\n";
		_exit(0) if fork // die "fork(): $!\n";
#		warn Dumper(init => @_);
	},
	getattr => sub {
#		warn Dumper(getattr => @_);
		use integer;
		my $path = shift;
		my $dentry = $share->get_entry($path);
		return - ENOENT unless $dentry;
		return dentry2stat($dentry);
	},
	readlink => sub {
#		warn Dumper(readlink => @_);
		my $path = shift;
		my $dentry = $share->get_entry($path);
		return ENOENT unless $dentry;
		return EINVAL unless $dentry->is_symlink;
		return $dentry->symlink;
	},
	access => sub {
#		warn Dumper(access => @_);
		return 0;
	},
#	getdir => sub {
#		warn Dumper(getdir => @_);
#		my $path = shift;
#		my $cursor = $share->ls($path);
#		return ENOENT unless $cursor->read;
#		my @res;
#		for(;;) {
#			my $dentry = $cursor->fetch;
#			last unless defined $dentry;
#			my $name = $dentry->name;
#			$name =~ s{^.*/}{}a;
#			push @res, $name;
#		}
#		warn Dumper(getdir_result => join(', ', @res));
#		return qw(. ..), @res, 0;
#	},
	opendir => sub {
		my $path = shift;
		my $cursor = $share->ls($path);
		return ENOENT unless $cursor->read;
		return 0, $cursor;
	},
	readdir => sub {
		my (undef, $off, $cursor) = @_;
		my @ls = ([0, '.', [dentry2stat($cursor->read)]], [0, '..']);
		while(my $dentry = $cursor->fetch) {
			my $name = $dentry->name;
			$name =~ s{^.*/}{}a;
			push @ls, [0, $name, [dentry2stat($dentry)]];
		}
		return @ls, 0;
	},
	open => sub {
#		warn Dumper(open => @_);
		my ($path, $mode) = @_;
		return EROFS if $mode & O_ACCMODE != O_RDONLY;
		my $dentry = $share->get_entry($path);
		return ENOENT unless $dentry;
		return EISDIR if $dentry->is_directory;
		return ENOSYS unless $dentry->is_file;
		return 0, $pool->reader(digests => $dentry->digests);
	},
	read => sub {
#		local $Data::Dumper::Maxdepth = 1;
#		warn Dumper(read => @_);
		my (undef, $len, $off, $fh) = @_;
		return ${$fh->pread($off, $len)};
	},
);

=pod

=encoding utf8

=head1 NAME

mount.fruitbak - mount a Fruitbak backup as a filesystem

=head1 SYNOPSIS

 mount -t fruitbak [-o fruitbak=/path/to/fruitbak] <hostname>:<backupnum>:<sharename>

=cut

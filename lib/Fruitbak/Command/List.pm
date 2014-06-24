=encoding utf8

=head1 NAME

Fruitbak::Command::List - implementation of Fruitbak CLI ls command

=head1 SYNOPSIS

 my $fbak = new Fruitbak(confdir => '/etc/fruitbak');
 my $cmd = new Fruitbak::Command::List(fbak => $fbak);
 $cmd->run(@ARGV);

=head1 DESCRIPTION

This class is the implementation of the ‘ls’ (or ‘list’) command of the Fruitbak
CLI tool.

As with all Fruitbak classes, any errors will throw an exception (using
‘die’). Use eval {} as required.

=cut

package Fruitbak::Command::List;

use v5.14;

use Fruitbak::Command -self;

use POSIX qw(strftime);
use Text::Tabs qw(expand);
use IO::Handle;
use Fruitbak::Util;

=head1 CONSTRUCTOR

The only required argument is ‘fbak’.

=cut

# Register this command with Fruitbak::Command:
BEGIN {
	$Fruitbak::Command::commands{ls} = [__PACKAGE__, "List hosts and backups"];
	$Fruitbak::Command::commands{list} = [__PACKAGE__];
}

=head1 FUNCTIONS

=over

=item width($string)

Calculates the width of a string, taking into account all sorts of terminal
and unicode vagaries. Does not (yet) deal with terminal escape codes.
Returns an integer.

=cut

sub width() {
	my $str = shift;
	# the order matters: the backspace code doesn't like combining
	# characters so they need to be removed. The cjk code doesn't know
	# how to deal with backspaces, and the tab code doesn't know how
	# to deal with any of the other special characters so they need to
	# be removed first.
	$str =~ s/(?:\p{Me}|\p{Mn})+//g;      # combining characters
	$str =~ s/(?:^|.)\010//gm;            # backspaces
	$str =~ s/(?:\p{Ea=W}|\p{Ea=F})/xx/g; # doublewidth cjk characters
	$str = expand($str);                  # tabs
	return length($str);
}

=item escapechars($string)

Escape control characters (and backslash) to either named or numeric
backslash escapes. Returns the escaped string.

=cut

my %escapechars = (
	"\t" => '\t',
	"\n" => '\n',
	"\r" => '\r',
	"\f" => '\f',
	"\b" => '\b',
	"\a" => '\a',
	"\e" => '\e',
	"\013" => '\v',
);

sub escapechars() {
	return $_[0] =~ s{\\}{\\\\}gra
		=~ s{([\0-\037])}{$escapechars{$1} // sprintf('\%03o', ord($1))}gera;
}

=back

=head1 METHODS

=over

=item format_table($rows, [$alignment])

Takes a list of rows (each of which is a list of columns) and returns a
string containing all rows and columns concatenated and properly aligned.
Each column is separated using two spaces. Columns are left-aligned unless
otherwise specified by the alignment parameter.

The optional alignment arrayref can be used to indicate that some columns
need to be right-aligned. It does not need to have the same length as the
columns in the table. A defined value at position I<n> will right-align
column I<n>, while an undefined value will make it use the default
left-alignment.

Right-aligned data with spaces in it is treated slightly differently:
everything up to and including the last space is left-aligned and only the
remainder is right-aligned. This feature is used, for example, when
displaying major/minor numbers for devices.

This function does not know how to deal with tab characters.

Returns the completely formatted string.

=cut

sub format_table {
	my $table = shift;
	my $align = shift // [];
	my @widths;
	my @numeric;
	foreach my $row (@$table) {
		while(my ($i, $col) = each @$row) {
			my $len = width($col // '');
			$widths[$i] = $len if $len > ($widths[$i] // -1);
		}
	}

	my $res = '';
	foreach my $row (@$table) {
		if(@$row) {
			my @row = @$row;
			my $last = pop @row;
			while(my ($i, $col) = each @row) {
				if(defined $align->[$i]) {
					my $pad = ' 'x($widths[$i] - width($col));
					my $padded = $col =~ s/^(?:.*\s)?\K/$pad/ra;
					$res .= $padded . '  ';
				} else {
					$res .= $col . ' 'x($widths[$i] - width($col) + 2);
				}
			}
			$res .= $last;
		}
		$res .= "\n";
	}
	return $res;
}

=item human_readable($number)

Given a (possibly large) number, will transform it into a human-readable
string. It appends SI units even though it uses base-2 math. Returns the
formatted string.

=cut

my @units = ('', qw(k M G T P E Z Y));
sub human_readable {
	use integer;
	my $num = shift;
	my $rest;
	my $index = 0;
	while($num >= 1000) {
		$rest = $num & 1023;
		$num >>= 10;
		$index++;
	}
	my $sub = '';
	if(defined $rest) {
		if($num < 10) {
			my $dec = ($rest * 10 + 512) >> 10;
			if($dec > 9) {
				$dec = 0;
				$num++;
			}
			$sub = ".$dec";
		} else {
			$num++ if $rest >= 512;
		}
	}
	return "$num$sub$units[$index]";
}

=item relative_path($from, $to)

Given two paths (typically that of a link and its target) returns the path
that you would need to traverse if the hypothetical current working
directory is the $from path's parent. Examples:

=over

=item relative_path(usr/share/man, usr/share/doc/man)

Returns: ‘doc/man’: the parent of usr/share/man is usr/share, and to access
usr/share/doc/man from usr/share you need to use ‘doc/man’.

=item relative_path(usr/bin/gzip, usr/bin/gunzip)

Returns: ‘gunzip’

=item relative_path(usr/lib/libfoo.so, usr/lib64/libfoo.so)

Returns: ‘../lib64/libfoo.so’

=back

=cut

sub relative_path {
	my ($from, $to) = @_;
	my @from = split(/\//, $from);
	my @to = split(/\//, $to);
	while($from[0] eq $to[0]) {
		shift @from;
		shift @to;
	}
	while(@from > 1) {
		shift @from;
		unshift @to, '..';
	}
	return join('/', @to);
}

=item format_dentry($dentry)

Given a Fruitbak::Dentry object, returns an arrayref with fields that,
when concatenated, look a lot like the output of ‘ls -l’.

=cut

sub format_dentry {
	my $dentry = shift;
	my @row;

	my $target = $dentry->target;
	my $is_hardlink = $dentry->is_hardlink;

	my $typechar = $dentry->is_directory ? 'd'
		: $dentry->is_symlink ? 'l'
		: $dentry->is_chardev ? 'c'
		: $dentry->is_blockdev ? 'b'
		: $dentry->is_fifo ? 'p'
		: $dentry->is_socket ? 's'
		: $is_hardlink ? 'h'
		: $dentry->is_file ? '-'
		: '?';

	my $name = $dentry->name;
	utf8_testandset_inplace($name);
	$name = '.' if $name eq '';
	$name =~ s{^.*/}{}a;
	$name = escapechars($name);
	if($is_hardlink) {
		$typechar = uc($typechar);
		my $link = $self->relative_path($dentry->name, $target->name);
		$name .= " => ".escapechars(utf8_testandset($link));
	} elsif($dentry->is_symlink) {
		$name .= " -> ".escapechars(utf8_testandset($dentry->symlink));
	}

	my $mode = $dentry->mode;
	my $modechars = sprintf('%s%s%s%s%s%s%s%s%s',
		($mode & 0400 ? 'r' : '-'),
		($mode & 0200 ? 'w' : '-'),
		($mode & 0100 ? ($mode & 04000 ? 's' : 'x') : ($mode & 04000 ? 'S' : '-')),
		($mode & 0040 ? 'r' : '-'),
		($mode & 0020 ? 'w' : '-'),
		($mode & 0010 ? ($mode & 02000 ? 's' : 'x') : ($mode & 02000 ? 'S' : '-')),
		($mode & 0004 ? 'r' : '-'),
		($mode & 0002 ? 'w' : '-'),
		($mode & 0001 ? ($mode & 01000 ? 't' : 'x') : ($mode & 01000 ? 'T' : '-')),
	);

	my $size = $dentry->is_device
		? $dentry->rdev_major . ', ' . $dentry->rdev_minor
		: $self->human_readable($dentry->size);

	return [
		$dentry->inode,
		"$typechar$modechars",
		$dentry->uid,
		$dentry->gid,
		$size,
		strftime('%Y-%m-%d %H:%M:%S', localtime($dentry->mtime)),
		$name,
	];
}

=item run

Parse the arguments to determine what needs to be listed (hosts, backups,
shares, paths, etc), generate the listing and print it to stdout (or
whatever the current filehandle is).

=back

=cut

sub run {
	my (undef, $hostname, $backupnum, $sharename, $path, $dummy) = @_;

	die "usage: fruitbak ls [<hostname> [<backup> [<share> [<path>]]]]\n"
		if defined $dummy;

	my $fbak = $self->fbak;

	my @table;
	my @align;

	if(defined $hostname) {
		my $host = $fbak->get_host($hostname);
		if(defined $backupnum) {
			my $backup = $host->get_backup($backupnum);
			if(defined $sharename) {
				@align[0, 2..4] = ('') x 10;
				my $share;
				if(defined $path) {
					$share = $backup->get_share($sharename);
				} else {
					($share, $path) = $backup->resolve_share($sharename);
				}
				#push @table, ["mode", "inum", "uid", "gid", "size", "mtime"];
				my $cursor = $share->ls($path);
				while(my $dentry = $cursor->fetch) {
					push @table, $self->format_dentry($dentry);
				}
			} else {
				my $shares = $backup->shares;
				push @table, ["Share name", "Mount point", "Start", "End", "Duration", "Status"];
				foreach my $sharename (@$shares) {
					my $share = $backup->get_share($sharename);
					push @table, [
						$sharename,
						$share->mountpoint,
						strftime('%Y-%m-%d %H:%M:%S', localtime($share->startTime)),
						strftime('%Y-%m-%d %H:%M:%S', localtime($share->endTime)),
						($share->endTime - $share->startTime).'s',
						$share->failed ? 'fail' : 'done',
					];
				}
			}
		} else {
			my $backups = $host->backups;
			push @table, ["Index", "Start", "End", "Duration", "Type", "Level", "Status"];
			foreach my $backupnum (@$backups) {
				my $backup = $host->get_backup($backupnum);
				push @table, [
					$backupnum,
					strftime('%Y-%m-%d %H:%M:%S', localtime($backup->startTime)),
					strftime('%Y-%m-%d %H:%M:%S', localtime($backup->endTime)),
					($backup->endTime - $backup->startTime).'s',
					$backup->full ? 'full' : 'incr', $backup->level, $backup->failed ? 'fail' : 'done',
				];
			}
		}
	} else {
		push @table, ["Host name", "Last backup", "Type", "Level", "Status"];
		my $hosts = $fbak->hosts;
		foreach my $hostname (@$hosts) {
			my @row = ($hostname);
			my $host = $fbak->get_host($hostname);
			my $backups = $host->backups;
			if(@$backups) {
				my $backup = $host->get_backup($backups->[-1]);
				push @row, strftime('%Y-%m-%d %H:%M:%S', localtime($backup->startTime)),
					$backup->full ? 'full' : 'incr', $backup->level, $backup->failed ? 'fail' : 'done';
			}
			push @table, \@row;
		}
	}

	select->binmode(':utf8');
	print $self->format_table(\@table, \@align)
		or die "write(): $!\n";

	return 0;
}

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

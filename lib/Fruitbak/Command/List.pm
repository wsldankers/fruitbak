=encoding utf8

=head1 NAME

Fruitbak::Command::Backup - implementation of CLI ls command

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

package Fruitbak::Command::List;

use autodie;
use v5.14;

use POSIX qw(strftime);
use Text::Tabs qw(expand);
use Fruitbak::Util;

use Fruitbak::Command -self;

sub width() {
	my $str = shift;
	# the order matters: the backspace code doesn't like combining
	# characters so they need to be removed. The cjk code doesn't know
	# how to deal with backspaces, and the tab code doesn't know how
	# to deal with any of the other special characters so they need to
	# be removed first.
	$str =~ s/(?:\p{Me}|\p{Mn})+//g;      # combining characters
	$str =~ s/(?:^|.)\010//gm;            # backspaces
	$str =~ s/(?:\p{Ea=W}|\p{Ea=F})/xx/g; # cjk characters
	$str = expand($str);                  # tabs
	return length($str);
}

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

BEGIN {
	$Fruitbak::Command::commands{ls} = [__PACKAGE__, "List hosts and backups"];
	$Fruitbak::Command::commands{'list'} = [__PACKAGE__];
}

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
				push @table, ["Share name"], map { [$_] } @$shares;
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
					$backup->full ? 'full' : 'incr', $backup->level, $backup->status,
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
					$backup->full ? 'full' : 'incr', $backup->level, $backup->status;
			}
			push @table, \@row;
		}
	}

	binmode STDOUT, ':utf8';
	print $self->format_table(\@table, \@align);

	return 0;
}

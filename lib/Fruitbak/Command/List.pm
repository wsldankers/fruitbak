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

use Fruitbak::Command -self;

sub format_table {
	my $table = shift;
	my $align = shift // [];
	my @widths;
	my @numeric;
	foreach my $row (@$table) {
		while(my ($i, $col) = each @$row) {
			my $len = length($col // '');
			$widths[$i] = $len if $len > ($widths[$i] // -1);
		}
	}
	my @fmts = ("\n", map {
		join('  ', (map { '%'.($align->[$_] // '-').($widths[$_] // 0).'s' } 0..$_-2), '%s')."\n";
	} 1..@widths);

	my $res = '';
	foreach my $row (@$table) {
		$res .= sprintf($fmts[@$row], map { $_ // '' } @$row);
	}
	return $res;
}

my @units = ('', qw(k M G T P E Z Y));
sub human_readable {
	my $num = shift;
	my $index = 0;
	while($num >= 1000) {
		$num >>= 10;
		$index++;
	}
	return "$num$units[$index]";
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

	my $original = $dentry->original;

	my $typechar = $dentry->is_directory ? 'd'
		: $dentry->is_symlink ? 'l'
		: $dentry->is_chardev ? 'c'
		: $dentry->is_blockdev ? 'b'
		: $dentry->is_fifo ? 'p'
		: $dentry->is_socket ? 's'
		: $original ? 'h'
		: $dentry->is_file ? '-'
		: '?';

	my $name = $dentry->name;
	$name = '.' if $name eq '';
	$name =~ s{^.*/}{}a;
	if($original) {
		$typechar = uc($typechar);
		$name .= " => ".$self->relative_path($dentry->name, $original->hardlink);
	} elsif($dentry->is_symlink) {
		$original = $dentry;
		$name .= " -> ".$dentry->symlink;
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

	return [
		$dentry->inode,
		"$typechar$modechars",
		$dentry->uid,
		$dentry->gid,
		$self->human_readable($dentry->size), 
		strftime('%Y-%m-%d %H:%M:%S', localtime($dentry->mtime)),
		$name
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
				unless(defined $path) {
					($path, my $share) = $backup->resolve_share($sharename)
						or die "unknown share '$sharename'\n";
					$sharename = $share;
				}
				my $share = $backup->get_share($sharename);
				my $files = $share->ls($path);
				#push @table, ["mode", "inum", "uid", "gid", "size", "mtime"];
				foreach my $filename (@$files) {
					my $dentry = $share->get_entry($filename, 1);
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
					$backup->type, $backup->level, $backup->status,
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
					$backup->type, $backup->level, $backup->status;
			}
			push @table, \@row;
		}
	}

	print $self->format_table(\@table, \@align);

	return 0;
}

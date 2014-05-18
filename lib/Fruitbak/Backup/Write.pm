=encoding utf8

=head1 NAME

Fruitbak::Backup::Write - write a new backup to disk

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

package Fruitbak::Backup::Write;

use Fcntl qw(:flock);
use JSON;
use IO::File;

use Fruitbak::Share::Write;

use Class::Clarity -self;

field dir => sub { $self->host->dir . '/new' };
field sharedir => sub { $self->dir . '/share' };
field host; # (Fruitbak::Host) required for new
field number => sub {
	my $backups = $self->host->backups;
	return @$backups ? $backups->[-1] + 1 : 0;
};
field fbak => sub { $self->host->fbak };
field shares => sub { $self->host->cfg->shares // ['/'] };
field status => 'failed';
field type => sub { $self->level ? 'incr' : 'full' };
field level => sub {
	my $ref = $self->refBackup;
	return $ref ? $ref->level + 1 : 0;
};
field startTime;
field endTime;
field info => sub {
	return {
		status => $self->status,
		level => $self->level,
		startTime => $self->startTime,
		endTime => $self->endTime
	};
};
field refBackup => sub {
	my $host = $self->host;
	my $backups = $host->backups;
	my $number = $backups->[-1];
	return undef unless defined $number;
	return $host->get_backup($number);
};

sub json_boolean() {
	return shift() ? JSON::true : JSON::false;
}

sub new() {
	my $self = super;

	my $dir = $self->dir;
	mkdir($dir) or $!{EEXIST} or
		die "mkdir($dir): $!\n";
	my $sharedir = $self->sharedir;
	mkdir($sharedir) or $!{EEXIST} or
		die "mkdir($sharedir): $!\n";
	$self->lock;
	$self->startTime(time);

	return $self;
}

# make sure only one process can access the new backup
field lock => sub {
	my $dir = $self->dir;

	my $lock = new IO::File("$dir/lock", '>')
		or die "open($dir/lock): $!\n";

	flock($lock, LOCK_EX|LOCK_NB)
		or die "A backup is already in progress for host '".$self->host->name."'\n";

	return $lock;
};

sub unlock {
	my $dir = $self->dir;
	unlink("$dir/lock") #or $!{ENOENT}
		or die "unlink($dir/lock): $!\n";
	$self->lock_reset;
}

sub run {
	my $shares = $self->shares;
	foreach my $sharename (@$shares) {
		my $share = new Fruitbak::Share::Write(name => $sharename, backup => $self);
		$share->run;
	}
	$self->finish;
}

# finish the backup and convert this object to a Fruitbak::Backup::Read
# FIXME: register this object with the host object it belongs to
sub finish {
	my $number = $self->number;
	my $host = $self->host;
	my $src = $self->dir;

	$self->status('done');
	$self->endTime(time);
	my $info = new IO::File("$src/info.json", '>')
		or die "open($src/info.json): $!\n";
	$info->write(encode_json($self->info))
		or die "write($src/info.json): $!\n";
	$info->flush or die "write($src/info.json): $!\n";
	$info->sync or die "write($src/info.json): $!\n";
	$info->close or die "write($src/info.json): $!\n";
	undef $info;

	my $dst = $self->host->dir . '/' . $self->number;
	rename($src, $dst)
		or die "rename($src, $dst): $!\n";
	$self->dir($dst);
	$self->unlock;
	bless $self, 'Fruitbak::Backup::Read';
}

=encoding utf8

=head1 NAME

Fruitbak::Backup::Write - Fruitbak class to create a new backup

=head1 SYNOPSIS

 my $fbak = new Fruitbak(confdir => '/etc/fruitbak');
 my $host = $fbak->get_host('pikachu');
 my $backup = $host->new_backup(full => 1);
 $backup->run;

=head1 DESCRIPTION

Use this class to create new backups, by instantiating it and calling the
run method.

As with all Fruitbak classes, any errors will throw an exception (using
‘die’). Use eval {} as required.

=head1 CONSTRUCTOR

Never use 

=cut

package Fruitbak::Backup::Write;

use Fcntl qw(:flock);
use JSON;
use IO::File;

use Fruitbak::Share::Write;

use Class::Clarity -self;

field dir => sub { $self->host->dir . '/new' };
field sharedir => sub { $self->dir . '/share' };
field host;
field number => sub {
	my $backups = $self->host->backups;
	return @$backups ? $backups->[-1] + 1 : 0;
};
field fbak => sub { $self->host->fbak };
field cfg => sub { $self->host->cfg };
field sharecfg => sub {
	my $shares = $self->cfg->shares // ['/'];
	my $cfg = $self->host->cfg;
	my $transfer = $cfg->transfer;
	my @transfer = (transfer => $transfer)
		if $transfer;
	return [map { ref $_ ? $_ : { name => $_, @transfer } } @$shares];
};
field shares => sub { [map { $_->{name} } @{$self->sharecfg}] };
field status => 'failed';
field full => sub { $self->level_isset ? !self->level : !$self->refbackup };
field level => sub {
	return 0 if $self->full;
	my $ref = $self->refbackup;
	return $ref ? $ref->level + 1 : 0;
};
field startTime;
field endTime;
field info => sub {
	return {
		status => $self->status,
		level => $self->level,
		startTime => $self->startTime,
		endTime => $self->endTime,
	};
};
field refbackup => sub {
	my $host = $self->host;
	my $backups = $host->backups;
	my $offset = -1;
	if($self->level_isset) {
		my $level = $self->level;
		if($level && !$self->full) {
			foreach my $b (@$backups) {
				my $l = $host->get_backup($b)->level;
				last if $l < $level;
			}
		}
	}
	my $number = $backups->[$offset];
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
	my $cfg = $self->cfg;
	my $name = self->host->name;
	local $ENV{name} = $name;
	local $ENV{host} = $cfg->host // $name;
	my $user = $cfg->user;
	local $ENV{user} = $user if defined $user;
	my $port = $cfg->port;
	local $ENV{port} = $port if defined $port;
	$self->startTime(time);
	my $shares = $self->sharecfg;
	foreach my $cfg (@$shares) {
		my $share = new Fruitbak::Share::Write(backup => $self, cfg => $cfg);
		$share->run;
	}
	$self->endTime(time);
	$self->finish;
}

# finish the backup and convert this object to a Fruitbak::Backup::Read
# FIXME: register this object with the host object it belongs to
sub finish {
	my $number = $self->number;
	my $host = $self->host;
	my $src = $self->dir;

	$self->status('done');
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

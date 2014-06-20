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

The only required argument is host, but don't call this constructor
directly. Always use $host->new_backup.

=cut

package Fruitbak::Backup::Write;

use Fcntl qw(:flock);
use JSON;
use IO::File;

use Fruitbak::Share::Write;

use Class::Clarity -self;

=head1 FIELDS

Fruitbak makes heavy use of Class::Clarity (the base class of this class).
One of the features of Class::Clarity is ‘fields’: elements of the object
hash with eponymous getters and setters. These fields can optionally have
an initializer: a function that is called when no value is yet assigned to
the hash element. For more information, see L<Class::Clarity>.

=over

=item field host

The host object that this backup belongs to. Should be set before calling
any methods ($host->new_backup will take of that). Do not modify
afterwards.

=cut

field host;

=item field dir

The path to the directory where this backup is being created. Because this
backup is new, it will be named ‘new’ until it is finished and moved into
its final location. Do not set.

=cut

field dir => sub { $self->host->dir . '/new' };

=item field sharedir

The path to the subdirectory of the backup directory that will contain the
shares. Do not set.

=cut

field sharedir => sub { $self->dir . '/share' };

=item field number

The number that this backup will have when it is finished. Do not set.

=cut

field number => sub {
	my $backups = $self->host->backups;
	return @$backups ? $backups->[-1] + 1 : 0;
};

=item field fbak

The Fruitbak object that this object (and its host) belong to. Do not set.

=cut

field fbak => sub { $self->host->fbak };

=item field cfg

The Fruitbak::Config::Host configuration object of the host that this
backup belongs to. Do not set.

=cut

field cfg => sub { $self->host->cfg };

=item field sharecfg

The configuration for the shares that will be backupped. It takes the
value from the configuration and expands any plain strings to hash
references. If a global transfer methods is found, this will be added to
each hash as well. If no shares were defined at all, it defaults to the
root (‘/’). See the documentation on configuring shares for more
information. Returns an arrayref. Do not set.

=cut

field sharecfg => sub {
	my $shares = $self->cfg->shares // ['/'];
	my $cfg = $self->host->cfg;
	my $transfer = $cfg->transfer;
	my @transfer = (transfer => $transfer)
		if $transfer;
	return [map { ref $_ ? $_ : { name => $_, @transfer } } @$shares];
};

=item field shares

The list of share names that will be backupped. Returns an arrayref. Do
not set.

=cut

field shares => sub { [map { $_->{name} } @{$self->sharecfg}] };

=item field status

Indicates the status of the backup, as it will be recorded when the backup
is finished. Defaults to ‘done’ and is set to ‘fail’ if any of the shares
failed during the backup. For internal use only.

=cut

field status => 'done';

=item field full

Boolean indicating whether this will be a full or incremental backup.
Do not change after calling any methods.

Never set both the ‘full’ and ‘level’ fields. Each will be computed from
the other as necessary.

=cut

field full => sub { $self->level_isset ? !self->level : !$self->refhostbackup };

=item field level

Integer indicating the (intended) level of the backup, which is the number
of steps it takes to traverse the chain of reference backups until you
reach a full backup. Do not change after calling any methods.

Never set both the ‘full’ and ‘level’ fields. Each will be computed from
the other as necessary.

=cut

field level => sub {
	return 0 if $self->full;
	my $ref = $self->refhostbackup;
	return $ref ? $ref->level + 1 : 0;
};

=item field refbackup

The backup object that this backup will be based on. Because each backup is
completely standalone, it is mostly a matter of performance which backup is
used.

For best performance, use the most similar backup to the one that is being
created. Usually that means the most recent backup of this host. This is
also (not unsurprisingly) the default. Because of this, you should rarely
have a need to supply a a reference backup yourself.

Note that even for full backups (where files are transferred
unconditionally) a reference backup is used to reduce the load on the pool.

The reference backup does not have to belong to the same host. Do not
change after calling any methods.

=cut

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

=item field refhostbackup

The same as refbackup, but only if the refbackup belongs to the same host.
Otherwise it is undef. Used for internal bookkeeping. Do not set.

=cut

field refhostbackup => sub {
	my $ref = $self->refbackup;
	return undef unless $ref;
	return undef if $ref->host->name ne $self->host->name;
	return $ref;
};

=item field startTime

The starting time of this backup, in seconds since the unix epoch. Does not
get a value until the backup starts. Do not set.

=cut

field startTime;

=item field endTime

The ending time of this backup, in seconds since the unix epoch. Does not
get a value until the backup finishes. Do not set.

=cut

field endTime;

=item field info

Various bits of information that will be written to the backup directory
after the backup is finished. For internal use only.

=cut

field info => sub {
	my $ref = $self->refbackup;
	my @ref = (ref => $ref->number)
		if $ref;
	my @refhost = (refhost => $ref->host->name)
		if $ref && !$self->refhostbackup;
	return {
		status => $self->status,
		level => $self->level,
		startTime => $self->startTime,
		endTime => $self->endTime,
		@ref,
		@refhost,
	};
};

=item field lock

Acquires and holds a lock on the backup directory, so as to prevent two
backups for the same host running concurrently. The lock is released
automatically when the object is destroyed or the backup is finished.
Do not set.

=cut

field lock => sub {
	my $dir = $self->dir;

	my $lock = new IO::File("$dir/lock", '>')
		or die "open($dir/lock): $!\n";

	flock($lock, LOCK_EX|LOCK_NB)
		or die "A backup is already in progress for host '".$self->host->name."'\n";

	return $lock;
};

=back

=head1 METHODS

=over

=item new

The constructor method for this class. Prepares the directory for use. Do
not call directly but always use $host->new_backup. See the CONSTRUCTOR section
for more information.

=cut

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

=item unlock

Unlocks the lock that was set using the lock field. May die if no lock was
held. For internal use only.

=cut

sub unlock {
	my $dir = $self->dir;
	unlink("$dir/lock") #or $!{ENOENT}
		or die "unlink($dir/lock): $!\n";
	$self->lock_reset;
}

=item run

Runs the actual backup. To this end it sets some environment variable (for
any shell commands that may run as part of the backup), records the start
and end time and calls the finish method. If any of the shares failed, it
sets the status field to ‘fail’;

=cut

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
		local $@;
		unless(eval { $share->run; 1 }) {
			warn $@;
			$self->status('fail');
		}
	}
	$self->endTime(time);
	$self->finish;
}

=item finish

Finalizes the backup and reblesses the object into the
Fruitbak::Backup::Read class. For internal use only.

=cut

# FIXME: register this object with the host object it belongs to
sub finish {
	my $number = $self->number;
	my $host = $self->host;
	my $src = $self->dir;

	$self->info_reset;
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

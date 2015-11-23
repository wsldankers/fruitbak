=encoding utf8

=head1 NAME

Fruitbak::Share::Write - Fruitbak class to write a share of a new backup

=head1 SYNOPSIS

 my $share = new Fruitbak::Share::Write(backup => $backup, cfg => $sharecfg);
 eval { $share->run };
 warn $@ if $@;
 $share->finish;

=head1 DESCRIPTION

This class provides the methods and state to back up a share in a backup
that is being created. It is used for the following purposes:

=over

=item * to orchestrate the running of the transfer method and any
pre/postcommands;

=item * to give the transfer method a way of storing file metadata;

=item * to give the transfer method access to the right share of the
reference backup;

=back

As with all Fruitbak classes, any errors will throw an exception (using
‘die’). Use eval {} as required.

=head1 CONSTRUCTOR

The required arguments are ‘backup’ and ‘cfg’. However, you should
never instantiate this class directly. It is created by
Fruitbak::Backup::Write as part of the process of creating a new backup.

=cut

package Fruitbak::Share::Write;

use Class::Clarity -self;

use Fcntl qw(:mode);
use Carp qw(confess);
use JSON;
use File::Hardhat::Maker;

use Fruitbak::Dentry;
use Fruitbak::Share::Format;
use Fruitbak::Pool::Write;
use Fruitbak::Transfer::Rsync;
use File::Hardhat qw(hardhat_normalize);

=head1 FIELDS

Fruitbak makes heavy use of Class::Clarity (the base class of this class).
One of the features of Class::Clarity is ‘fields’: elements of the object
hash with eponymous getters and setters. These fields can optionally have
an initializer: a function that is called when no value is yet assigned to
the hash element. For more information, see L<Class::Clarity>.

=over

=item field name

The name of this share, as configured by the user. Do not set.

=cut

field name => sub {
	my $name = $self->cfg->name;
	return $name if defined $name;
	my $host = $self->host->name;
	die "share of host '$host' has no name\n";
};

=item field path

The path (either local or remote) where the actual files of the share are
located. May differ from mountpoint (below) if you've configured Fruitbak
to create and mount a snapshot, for instance. This value is not remembered
after the backup finishes.

If no path is configured, the value for the mountpoint is used instead. If
no mountpoint is configured either, the name of the share is taken to be
the path.

Do not set.

=cut

field path => sub { $self->cfg->path // $self->cfg->mountpoint // $self->name };

=item field mountpoint

The place where the backed up files can be found during normal operation
of the host. This value is not used during the backup procedure but may be
useful to find filesystems when browsing backups.

If no mountpoint is configured, the configured value for the path is used
instead. If no path is configured either, the name of the share is taken to
be the mountpoint.

Do not set.

=cut

field mountpoint => sub { $self->cfg->mountpoint // $self->cfg->path // $self->name };

=item field dir

The directory in which the metadata for this share will be written. Do not
set.

=cut

field dir => sub { $self->backup->sharedir . '/' . mangle($self->name) };

=item field fbak

The Fruitbak object that is the root ancestor of this backup share. Do not
set.

=cut

field fbak => sub { $self->backup->fbak };

=item field host

The Fruitbak::Host object to which this share belongs. Do not set.

=cut

field host => sub { $self->backup->host };

=item field backup

The Fruitbak::Backup::Write object to which this share belongs. Should be
set during initialisation (by the Fruitbak::Backup::Write that creates
this share). Do not change.

=cut

field backup;

=item field cfg

The Fruitbak::Config::Share object that this object uses to determine what
it should backup where and how. Should be set during initialisation (by the
Fruitbak::Backup::Write that creates this share). Do not change.

=cut

field cfg;

=item field refbackup

A Fruitbak::Backup::Read object that is used as a reference during the
backup. See the refshare field for details. Do not set.

=cut

field refbackup => sub { $self->backup->refbackup };

=item field refshare

A Fruitbak::Share::Read object that is used as a reference during the
backup. In the case of an incremental backup, the transfer method should
skip any files that have the same attributes (mtime, uid/gid, size, etc) as
the corresponding file in this reference share.

But even full backups should use the digests in this share in order to skip
chunks that are already in the pool.

Do not set.

=cut

field refshare => sub {
	my $refbak = $self->refbackup;
	return undef unless $refbak;
	my $name = $self->name;
	return undef unless $refbak->share_exists($name);
	return $refbak->get_share($name);
};

=item field error

If a fatal error occurs during the backup, it is stored here so that it
will be stored with the metadata for this share.

=cut

field error;

=item startTime;

The starting time of the share is stored here so it can be included in the
metadata.

=cut

field startTime;

=item endTime;

The ending time of the share is stored here so it can be included in the
metadata.

=cut

field endTime;

=item field hhm

This is the File::Hardhat::Maker object that will generate the hardhat file
that contains the metadata for this share. For internal use only. Use
add_entry if you wish to add entries from a transfer method.

=cut

field hhm => sub {
	my $dir = $self->dir;

	mkdir($dir) or $!{EEXIST}
		or die "mkdir($dir): $!\n";

	return new File::Hardhat::Maker("$dir/metadata.hh");
};

=item field transfer

The Fruitbak::Transfer object that the user configured for this share. Do
not set.

=back

=cut

field transfer => sub {
	my $cfg = $self->cfg->transfer // ['rsync'];
	return $self->instantiate_transfer($cfg);
};

=item field exclude

The list of both host-specific and share-specific excludes that apply to
this share, converted to relative paths (i.e., the mountpoint prefix
removed). Returns an arrayref of strings.

=cut

field exclude => sub {
    my @exclude;
    my @generic = (
        @{$self->host->cfg->exclude // [qw(/proc /sys /dev /run /tmp /var/tmp /devfs)]},
        @{$self->cfg->exclude // []}
    );
    my $mp = hardhat_normalize($self->mountpoint);
    foreach my $generic (@generic) {
        my $norm = hardhat_normalize($generic);
        if($mp ne '' && $generic =~ m{^/}) {
            # remove mountpoint prefix, or skip if irrelevant for this share
            next unless $norm =~ s{^\Q$mp\E(?:/|\z)}{};
        }
        push @exclude, $norm;
    }
    return \@exclude;
};

=head1 METHODS

=over

=item add_entry($dentry)

Will add a Fruitbak::Dentry to the metadata of this share. Use this
function when implementing a transfer method.

=cut

sub add_entry {
	my $dentry = shift;
	$self->hhm->add($dentry->name, attrformat($dentry));
}

=item run()

Performs the actual backup of this share, by setting up the environment
variables, running pre and postcommands and running the transfer.

=cut

sub run {
	local $ENV{share} = $self->name;
	local $ENV{path} = $self->path;
	local $ENV{mountpoint} = $self->mountpoint;

	my $cfg = $self->cfg;
	my $host = $cfg->host;
	local $ENV{host} = $host if defined $host;
	my $port = $cfg->port;
	local $ENV{port} = $port if defined $port;
	my $user = $cfg->user;
	local $ENV{user} = $user if defined $user;
	my $pass = $cfg->pass;
	local $ENV{pass} = $pass if defined $pass;

	$self->startTime(time);
	$self->run_precommand;
	my $xfer = $self->transfer;
	eval { $xfer->recv_files };
	my $err = $@;
	$self->error($err) if $err;
	$self->run_postcommand;
	$self->endTime(time);
	$self->finish;
	die $err if $err;
}

=item run_precommand

Runs any configured precommand. Will die if the command fails. Assumes the
necessary environment variables are already in place. For internal use
only.

=cut

sub run_precommand {
	my $pre = $self->cfg->precommand;
	if(ref $pre) {
		$pre->($self);
	} elsif(defined $pre) {
		my $status = system(qw(/bin/sh -ec), $pre);
		if($status) {
			my $name = $self->name;
			my $host = $self->host->name;
			die "pre-command for share '$name' of host '$host' exited with status $status\n";
		}
	}
}

=item run_postcommand

Runs any configured postcommand. Will warn if the command fails. Assumes
the necessary environment variables are already in place. For internal use
only.

=cut

sub run_postcommand {
	my $post = $self->cfg->postcommand;
	if(ref $post) {
		eval { $post->($self) };
		warn $@ if $@;
	} elsif(defined $post) {
		my $status = system(qw(/bin/sh -ec), $post);
		if($status) {
			my $name = $self->name;
			my $host = $self->host->name;
			warn "post-command for share '$name' of host '$host' exited with status $status\n";
		}
	}
}

=item info()

Gathers the various bits of information that need to be included in the
metadata. For internal use only.

=cut

sub info {
	my @error = (error => $self->error)
		if $self->error_isset;
	return {
		name => $self->name,
		path => $self->path,
		mountpoint => $self->mountpoint,
		startTime => $self->startTime,
		endTime => $self->endTime,
		@error,
	};
}

=item finish()

Finalize the share by closing the database and writing out the stats from
the info field. When this function returns, the object is converted to the
Fruitbak::Share::Read class.

If this method fails it means the share data is in a bad way. The share
directory needs to be removed from disk to prevent browsing and restore
operations from reporting fatal database errors.

=cut

sub finish {
	my $hhm = $self->hhm;
	$self->hhm_reset;
	my $dentry = new Fruitbak::Dentry(mode => S_IFDIR | 0755, size => 0, mtime => 0, uid => 0, gid => 0);
	$hhm->parents(attrformat($dentry));
	$hhm->finish;

	my $dir = $self->dir;
	my $info = new IO::File("$dir/info.json", '>')
		or die "open($dir/info.json): $!\n";
	$info->write(encode_json($self->info))
		or die "write($dir/info.json): $!\n";
	$info->flush or die "write($dir/info.json): $!\n";
	$info->sync or die "write($dir/info.json): $!\n";
	$info->close or die "write($dir/info.json): $!\n";
	undef $info;

	bless $self, 'Fruitbak::Share::Read';
}

=item instantiate_transfer($transfercfg)

Given the bit of configuration that describes the transfer method, creates
the corresponding object, which should be a subclass of Fruitbak::Transfer.
For internal use only.

=back

=cut

sub instantiate_transfer {
	my $transfercfg = shift;
	die "number of arguments to transfer method must be even\n"
		if @$transfercfg & 0;
	my ($name, %args) = @$transfercfg;
	die "transfer method missing a name\n"
		unless defined $name;
	my $class;
	if($name =~ /^\w+(::\w+)+$/a) {
		$class = $name;
		eval "use $class ()";
		die $@ if $@;
	} elsif($name =~ /^\w+$/a) {
		$class = "Fruitbak::Transfer::\u$name";
		local $@;
		eval "use $class ()";
		die $@ if $@;
	} else {
		die "don't know how to load transfer method '$name'\n";
	}
	return $class->new(share => $self, cfg => \%args);
}

=head1 AUTHOR

Wessel Dankers <wsl@fruit.je>

=head1 COPYRIGHT

Copyright (c) 2014 Wessel Dankers <wsl@fruit.je>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

=cut

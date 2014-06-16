=encoding utf8

=head1 NAME

Fruitbak::Share::Write - Fruitbak class to write a share of a new backup

=cut

package Fruitbak::Share::Write;

use Class::Clarity -self;

use Fcntl qw(:mode);
use Carp qw(confess);
use JSON;
use File::Hardhat::Maker;

use Fruitbak::Share::Format;
use Fruitbak::Pool::Write;
use Fruitbak::Transfer::Rsync;

field name => sub { $self->cfg->{name} // die "share has no name" };
field path => sub { $self->cfg->{path} // $self->name };
field dir => sub { $self->backup->sharedir . '/' . mangle($self->name) };
field fbak => sub { $self->backup->fbak };
field backup;
field cfg;
field refbackup => sub { $self->backup->refbackup };
field refshare => sub {
    my $refbak = $self->refbackup;
    return undef unless $refbak;
    return $refbak->get_share($self->name); 
};
field startTime;
field endTime;
field info => sub {
	return {
		name => $self->name,
		path => $self->path,
		startTime => $self->startTime,
		endTime => $self->endTime,
	};
};
field hhm => sub {
	my $dir = $self->dir;

	mkdir($dir) or $!{EEXIST}
		or die "mkdir($dir): $!\n";

	return new File::Hardhat::Maker("$dir/metadata.hh");
};

# add a Fruitbak::Dentry to the database
sub add_entry {
	my $dentry = shift;
	$self->hhm->add($dentry->name, attrformat($dentry));
}

sub run {
	local $ENV{share} = $self->name;
	local $ENV{path} = $self->path;
	my $xfer = $self->transfer;
	$self->startTime(time);
	$xfer->recv_files;
	$self->endTime(time);
	$self->finish;
}

# finish the share and convert this object to a Fruitbak::Share::Read
# FIXME: register this object with the backup object it belongs to
sub finish {
	my $hhm = $self->hhm;
	$self->hhm_reset;
	$hhm->parents;
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

field transfer => sub {
	my $cfg = $self->cfg->{transfer} // ['rsync'];
	return $self->instantiate_transfer($cfg);
};

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

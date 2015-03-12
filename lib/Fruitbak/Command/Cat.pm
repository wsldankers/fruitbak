=encoding utf8

=head1 NAME

Fruitbak::Command::Cat - implementation of CLI cat command

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

package Fruitbak::Command::Cat;

use Fruitbak::Command -self;

BEGIN {
	$Fruitbak::Command::commands{cat} = [__PACKAGE__, "Write a file to stdout"];
}

sub run {
	my (undef, $hostname, $backupnum, $sharename, $path) = @_;

	die "usage: fruitbak cat <hostname> <backup> <share> <path>\n"
		unless defined $sharename;

	my $fbak = $self->fbak;

	my $host = $fbak->get_host($hostname);
	my $backup = $host->get_backup($backupnum);
	my $share;
	if(defined $path) {
		$share = $backup->get_share($sharename);
	} else {
		($share, $path) = $backup->resolve_share($sharename);
	}
	my $dentry = $share->get_entry($path)
		or die "'$path': file not found\n";
	die "'$path' is not a file\n"
		unless $dentry->is_file;
	my $reader = $fbak->pool->reader(digests => $dentry->digests);

	my $buf = $reader->read;
	die "refusing to write a binary file to a terminal\n"
		if -t \*STDOUT && $$buf =~ /\0/a;
	binmode STDOUT;
	while($$buf ne '') {
		print $$buf or die "write(): $!\n";
		$buf = $reader->read;
	}

	return 0;
}

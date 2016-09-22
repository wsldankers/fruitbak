=encoding utf8

=head1 NAME

Fruitbak::Command::GC - implementation of CLI gc command

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

package Fruitbak::Command::GC;

no utf8;

use Fruitbak::Command -self;

use MIME::Base64;
use File::Hashset;
use IO::Pipe;
use POSIX qw(_exit);

BEGIN {
	$Fruitbak::Command::commands{gc} = [__PACKAGE__, "Clean up unused pool chunks"];
}

sub saferead() {
	my $fh = shift;
	my $num = shift;
	my $len = 0;
	my $res = '';
	
	while($len < $num) {
		my $r = sysread $fh, $res, $num - $len, $len;
		die "read(): $!\n" unless defined $r;
		return undef unless $r;
		confess("short read ($len < $num)") unless $r;
		$len = length($res);
	}
	return $res;
}

sub run {
	my (undef, $dummy) = @_;

	die "usage: fruitbak gc\n"
		if defined $dummy;

	my $fbak = $self->fbak;

	my $lock = $fbak->lock;

	my $hosts = $fbak->hosts;
	foreach my $name (@$hosts) {
		my $host = $fbak->get_host($name);
		my $expired = $host->expired($host);
		foreach my $e (@$expired) {
#			warn "removing $name/$e\n";
			$host->remove_backup($e);
		}
	}

	my $pipe = new IO::Pipe;
	my $pid = fork;
	unless($pid) {
		die "fork(): $!\n" unless defined $pid;
		eval {
			local $SIG{PIPE} = 'IGN';
			$pipe->reader;
			my $fbak = $fbak->clone;
			$self->fbak($fbak);
			my $pool = $fbak->pool;
			my $hashsize = $pool->hashsize;
			while(my $hash = saferead($pipe, $hashsize)) {
				$pool->remove($hash);
			}
		};
		if($@) {
			warn $@;
			_exit(1);
		}
		_exit(0);
		for(;;) { kill KILL => $$ }
	}
	$pipe->writer;

	my $pool = $fbak->pool;
	my $iterator = $pool->iterator;
	my $referenced = $fbak->hashes;
	my $total = 0;
	my $removed = 0;
	my $lost = 0;
	my $rootdir = $fbak->rootdir;
	my $hashsize = $pool->hashsize;

	my $foundfile = "$rootdir/available";
	my $found = new IO::File($foundfile, '>')
		or die "open($foundfile): $!\n";

	while(my $hashes = $iterator->fetch) {
		foreach my $hash (@$hashes) {
			if($referenced->exists($hash)) {
				$found->write($hash) or die "write($foundfile): $!\n";
			} else {
#				warn encode_base64($hash);
				my $r = syswrite($pipe, $hash);
				die "write(): $!\n" unless defined $r;
				# POSIX guarantees that no partial writes will occur,
				# assuming $hashsize <= PIPE_BUF
				confess("short write ($r < $hashsize)") if $r < $hashsize;
				$removed++;
			}
			$total++;
		}
	}
	$pipe->flush or die "write(pipe): $!\n";
	$pipe->close or die "close(pipe): $!\n";

	$found->flush or die "flush($foundfile): $!\n";
	$found->sync or die "fsync($foundfile): $!\n";
	$found->close or die "close($foundfile): $!\n";
	File::Hashset->sortfile($foundfile, $pool->hashsize);
	my $foundhashes = File::Hashset->load($foundfile, $hashsize);

	unlink($foundfile) or die "unlink($foundfile): $!\n";

	my $missingfile = "$rootdir/missing";
	my $missing = new IO::File("$missingfile.new", '>')
		or die "open($missingfile.new): $!\n";

	$iterator = $referenced->iterator;
	while(my $hash = $iterator->fetch) {
		unless($foundhashes->exists($hash)) {
			$missing->write($hash) or die "write($missingfile): $!\n";
			$lost++;
		}
	}

	$missing->flush or die "flush($missingfile.new): $!\n";
	$missing->sync or die "fsync($missingfile.new): $!\n";
	$missing->close or die "close($missingfile.new): $!\n";
	File::Hashset->sortfile("$missingfile.new", $hashsize);
	rename("$missingfile.new", $missingfile)
		or die "rename($missingfile.new, $missingfile): $!\n";

#	warn "removed $removed out of $total pool files\n";
	warn "detected $lost missing chunks!\n"
		if $lost;

	waitpid $pid, 0;
	return $?;
}

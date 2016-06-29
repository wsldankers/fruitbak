=encoding utf8

=head1 NAME

Fruitbak::Command::Scrub - implementation of CLI scrub command

=head1 AUTHOR

Wessel Dankers <wsl@fruit.je>

=head1 COPYRIGHT

Copyright (c) 2014,2016 Wessel Dankers <wsl@fruit.je>

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

package Fruitbak::Command::Scrub;

no utf8;

use Fruitbak::Command -self;

use MIME::Base64;
use POSIX qw(:sys_wait_h _exit);
use Math::BigInt;

BEGIN {
	$Fruitbak::Command::commands{scrub} = [__PACKAGE__, "Check pool data for damage"];
}

sub run {
	my (undef, $numprocs, $dummy) = @_;

	die "usage: fruitbak gc [numprocs]\n"
		if defined $dummy;

	$numprocs //= 1;
	$numprocs = int($numprocs);

	my $fbak = $self->fbak;
	my $pool = $fbak->pool;
	my $hashes = $fbak->hashes;
	my $hashalgo = $pool->hashalgo;
	my $hashsize = $pool->hashsize;
	my $hexlen = 2 * $hashsize;
	my $pad = '00' x $hexlen;
	my $proto = new Math::BigInt("0x1$pad");

	my $hash_search_boundary = sub {
		my ($index, $total) = @_;

		my $tel = $proto->copy;

		$tel->bmul($index);
		$tel->bdiv($total);

		# remove 0x then leftpad with zeros
		return pack('H*', substr($pad.substr($tel->as_hex, 2), -$hexlen));
	};

	my @pids;

	foreach my $tid (1..$numprocs) {
		if($numprocs > 1) {
			my $pid = fork;
			die "fork(): $!\n" unless defined $pid;
			if($pid) {
				push @pids, $pid;
				next;
			}
		}

		my $fail = 0;
		local $@;
		unless(eval {
			my $first = $hash_search_boundary->($tid - 1, $numprocs);
			my $last = $hash_search_boundary->($tid, $numprocs)
				if $tid < $numprocs;

			my $iterator = $hashes->iterator($first);

			while(my $digest = $iterator->fetch) {
				last if $last && $digest ge $last;
#				warn "$tid ".encode_base64($digest);
				my $data = eval { $pool->retrieve($digest) };
				unless($data) {
					print "while reading ".encode_base64($digest, '').": $@\n"
						or die "write(): $!\n";
					$fail = 1;
				} elsif($hashalgo->($$data) ne $digest) {
					print encode_base64($digest, '')."\n"
						or die "write(): $!\n";
					$fail = 1;
				}
			}

			return 1;
		}) {
			if($numprocs > 1) {
				warn $@;
			} else {
				die $@;
			}
		}
		_exit($fail) if $numprocs > 1;
	}

	my $fail = 0;

	if($numprocs > 1) {
		my $signaled;
		my $term = sub { $signaled = 1; kill TERM => @pids };
		local $SIG{INT} = $term;
		local $SIG{TERM} = $term;
		for(;;) {
			my $pid = waitpid(-1, 0);
			last if $pid == -1;
			next if $signaled;
			if(WIFEXITED($?)) {
				$fail = 1 if WEXITSTATUS($?);
			} elsif(WIFSIGNALED($?)) {
				$fail = 1;
				my $sig = WTERMSIG($?);
				warn sprintf("sub-process killed with signal %d%s\n",
					$sig & 127, ($sig & 128) ? ' (core dumped)' : '');
			}
		}
	}

	return $fail;
}

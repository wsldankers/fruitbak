=encoding utf8

=head1 NAME

Fruitbak::Command::Scrub - implementation of CLI scrub command

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

package Fruitbak::Command::Scrub;

no utf8;

use Fruitbak::Command -self;

use MIME::Base64;
use POSIX qw(:sys_wait_h _exit);

BEGIN {
	$Fruitbak::Command::commands{scrub} = [__PACKAGE__, "Check pool data for damage"];
}

sub run {
	my (undef, $numthreads, $dummy) = @_;

	die "usage: fruitbak gc [numthreads]\n"
		if defined $dummy;

	$numthreads //= 1;
	$numthreads = int($numthreads);

	my $fbak = $self->fbak;
	my $pool = $fbak->pool;
	my $iterator = $pool->iterator;
	my $hashalgo = $pool->hashalgo;

	foreach my $tid (1..$numthreads) {
		if($numthreads > 1) {
			my $pid = fork;
			die "fork(): $!\n" unless defined $pid;
			next if $pid;
		}

		my $fail = 0;
		local $@;
		unless(eval {
			my $first = int(2**32 * ($tid - 1) / $numthreads);
			my $last = int(2**32 * $tid / $numthreads);

			while(my $digests = $iterator->fetch) {
				foreach my $digest (@$digests) {
					my $slice = unpack(L => $digest);
					next if $slice < $first;
					next if $slice >= $last;
	#				warn "$tid ".encode_base64($digest);
					my $data = $pool->retrieve($digest);
					unless($hashalgo->($$data) eq $digest) {
						print encode_base64($digest)
							or die "write(): $!\n";
						$fail = 1;
					}
				}
			}

			return 1;
		}) {
			if($numthreads > 1) {
				eval { warn $@ };
			} else {
				die $@;
			}
		}
		_exit($fail) if $numthreads > 1;;
	}

	my $fail = 0;

	if($numthreads > 1) {
		for(;;) {
			my $pid = waitpid(-1, 0);
			last if $pid == -1;
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

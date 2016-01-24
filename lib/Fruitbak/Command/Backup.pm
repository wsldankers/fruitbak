=encoding utf8

=head1 NAME

Fruitbak::Command::Backup - implementation of CLI help command

=head1 AUTHOR

Wessel Dankers <wsl@fruit.je>

=head1 COPYRIGHT

Copyright (c) 2014,2015 Wessel Dankers <wsl@fruit.je>

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

package Fruitbak::Command::Backup;

use Fruitbak::Command -self;

use Time::HiRes qw(time);
use POSIX qw(:sys_wait_h _exit);
use Fruitbak::Util qw(parse_interval);

BEGIN {
	$Fruitbak::Command::commands{backup} = [__PACKAGE__, "Run a single backup"];
	$Fruitbak::Command::commands{bu} = [__PACKAGE__];
}

sub options {
	return [
		@{super()},
		["f|full:s", undef, "Run a full backup (optionally)"],
	];
}

# true if defined (but may be 0!)
field full => undef;

sub opt_f {
	my $full = shift;
	if($full eq '') {
		$self->full(0);
	} else {
		die "while parsing --full='$full': $@"
			unless eval { $self->full(parse_interval($full)); 1 };
	}
}

sub do_one_host {
	my ($hostname, $full, $auto) = @_;

	my $fbak = $self->fbak;
	my $host = $fbak->get_host($hostname);
	return if $auto && !($host->cfg->auto // 1);
	if(my $last = $host->get_backup) {
		if(defined $full) {
			$full = $last->startTime < time() - $full;
		} else {
			$full = 0;
		}
	} else {
		$full = 1;
	}
	my $bu = $host->new_backup(full => $full);
	return $bu->run;
}

sub run {
	my (undef, @hostnames) = @_;

	my $full = $self->full;

	my $fbak = $self->fbak;
	my $cfg = $fbak->cfg;

	my $lock = $fbak->lock(1);
	my $fail = 0;

	my $auto = !@hostnames;

	@hostnames = @{$fbak->cfg->hosts} unless @hostnames;
	die "no hosts configured\n" unless @hostnames;

	my $maxjobs = int($cfg->maxjobs // 1);
	$maxjobs = 1 if $maxjobs < 1;

	if(@hostnames == 1 || $maxjobs == 1) {
		while(@hostnames) {
			eval {
				my $hostname = shift @hostnames;
				die "'$hostname' is not a valid host name\n"
					unless Fruitbak::Host->is_valid_name($hostname);
				my $exists = $fbak->host_exists($hostname);
				die "host '$hostname' is unknown\n"
					unless $exists;
				die "host '$hostname' is unconfigured\n"
					unless $exists > 1;

				$self->do_one_host($hostname, $full, $auto);
			};
			if($@) {
				$fail = 1;
				warn $@;
			}
		}
	} else {
		local $SIG{CHLD} = 'DEFAULT';
		my $curjobs = 0;
		my %jobs;
		while(@hostnames || %jobs) {
			while(@hostnames && keys %jobs < $maxjobs) {
				my $hostname = shift @hostnames;
				eval {
					die "'$hostname' is not a valid host name\n"
						unless Fruitbak::Host->is_valid_name($hostname);
					my $exists = $fbak->host_exists($hostname);
					die "host '$hostname' is unknown\n"
						unless $exists;
					die "host '$hostname' is unconfigured\n"
						unless $exists > 1;
				};
				if($@) {
					warn $@;
					next;
				}

				my $pid = fork();
				if($pid) {
					$jobs{$pid} = $hostname;
				} elsif(defined $pid) {
					$self->fbak($fbak->clone);
					eval { $self->do_one_host($hostname, $full, $auto) };
					if($@) {
						warn $@;
						_exit(1);
					}
					_exit(0);
					die;
				} else {
					$fail = 1;
					warn "fork(): $!\n";
					@hostnames = ();
				}
			}
			my $pid = waitpid(-1, 0);
			die "waitpid returns -1 when there should still be running processes?!\n"
				if $pid == -1;
			if(exists $jobs{$pid}) {
				my $hostname = delete $jobs{$pid};
				if(WIFEXITED($?)) {
					$fail = 1 if WEXITSTATUS($?);
				} elsif(WIFSIGNALED($?)) {
					$fail = 1;
					my $sig = WTERMSIG($?);
					warn sprintf("sub-process for '$hostname' killed with signal %d%s\n",
						$sig & 127, ($sig & 128) ? ' (core dumped)' : '');
				}
			} elsif(WIFSIGNALED($?)) {
				my $sig = WTERMSIG($?);
				warn sprintf("sub-process killed with signal %d%s\n",
					$sig & 127, ($sig & 128) ? ' (core dumped)' : '');
			}
		}
	}

	return $fail;
}

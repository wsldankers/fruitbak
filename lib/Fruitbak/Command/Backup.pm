=encoding utf8

=head1 NAME

Fruitbak::Command::Backup - implementation of CLI help command

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

package Fruitbak::Command::Backup;

use Fruitbak::Command -self;

use POSIX qw(_exit);

BEGIN {
	$Fruitbak::Command::commands{backup} = [__PACKAGE__, "Run a single backup"];
	$Fruitbak::Command::commands{'bu'} = [__PACKAGE__];
}

sub run {
	my (undef, @hostnames) = @_;

	my $fbak = $self->fbak;
	my $cfg = $fbak->cfg;

	my $lock = $fbak->lock(1);
	my $fail = 0;

	@hosts = @{$fbak->cfg->hosts} unless @hosts;
	die "no hosts configured\n" unless @hosts;

	if(@hosts == 1) {
		die "'$hostname' is not a valid host name\n"
			unless Fruitbak::Host::is_valid_name($hostname);
		my $exists = $fbak->host_exists($hostname);
		die "host '$hostname' is unknown\n"
			unless $exists;
		die "host '$hostname' is unconfigured\n"
			unless $exists > 1;

		my $host = $fbak->get_host($hostname);
		my $bu = $host->new_backup;
		$bu->run;
	} else {
		local $SIG{CHLD} = 'DEFAULT';
		my $curjobs = 0;
		my $maxjobs = int($cfg->maxjobs // 1);
		$maxjobs = 1 if $maxjobs < 1;
		my %jobs;
		while(@hosts || $curjobs) {
			while(@hosts && $curjobs < $maxjobs) {
				my $hostname = shift @hosts;
				eval {
					die "'$hostname' is not a valid host name\n"
						unless Fruitbak::Host::is_valid_name($hostname);
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
					$curjobs++;
					$jobs{$pid} = $hostname;
				} elsif(defined $pid) {
					eval {
						my $fbak = $fbak->clone;
						my $host = $fbak->get_host($hostname);
						my $bu = $host->new_backup;
						$bu->run;
					};
					if($@) {
						warn $@;
						_exit(1);
					}
					_exit(0);
					die;
				} else {
					$fail = 1;
					warn "fork(): $!\n";
					@hosts = ();
				}
			}
			waitpid ← hier
		}
	}

	return $fail;
}

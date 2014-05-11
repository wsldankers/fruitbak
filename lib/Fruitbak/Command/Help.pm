=encoding utf8

=head1 NAME

Fruitbak::Command::Help - implementation of CLI help command

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

package Fruitbak::Command::Help;

use autodie;

use Fruitbak::Command -self;

BEGIN {
	$Fruitbak::Command::commands{help} = [__PACKAGE__, "Shows a list of commands"];
	$Fruitbak::Command::commands{'--help'} =
	$Fruitbak::Command::commands{'-h'} = [__PACKAGE__];
}

sub run {
	foreach my $cmd (sort keys %Fruitbak::Command::commands) {
		my ($class, $help) = @{$Fruitbak::Command::commands{$cmd}};
		next unless defined $help;
		printf("%-10s  %s\n", $cmd, $help);
	}
	return 0;
}

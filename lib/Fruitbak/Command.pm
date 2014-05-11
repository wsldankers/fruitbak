=encoding utf8

=head1 NAME

Fruitbak::Command - implementation of CLI commands

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

package Fruitbak::Command;

use Class::Clarity -self;

use Fruitbak;
use Fruitbak::Command::Backup;
use Fruitbak::Command::Init;
use Fruitbak::Command::Help;

# classes may register commands here as they are loaded:
our %commands;

field fbak;

sub run {
	my $exitcode = 0;
	local $SIG{__WARN__} = sub { $exitcode = 1; warn @_ };
	my $info = $commands{$_[0] // 'help'};

	unless($info) {
		warn "Unknown command '$_[0]'\n";
		$info = $commands{help};
	}

	my ($class) = @$info;

	my $cmd = $class->new(fbak => $self->fbak);

	return $cmd->run(@_) || $exitcode;
}
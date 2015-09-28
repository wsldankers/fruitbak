=encoding utf8

=head1 NAME

Fruitbak::Command - implementation of CLI commands

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

package Fruitbak::Command;

use Class::Clarity -self;

use Scalar::Util qw(reftype);
use Getopt::Long;

use Fruitbak;

# classes may register commands here as they are loaded:
our %commands;

use Fruitbak::Command::Backup;
use Fruitbak::Command::Cat;
use Fruitbak::Command::GC;
use Fruitbak::Command::Help;
use Fruitbak::Command::Init;
use Fruitbak::Command::List;
use Fruitbak::Command::Scrub;
use Fruitbak::Command::Tar;

field fbak;

sub options {
	return [
		"h|help|usage\0Display usage information for this command"
	]
}

sub opt_h {
	my @options = @{$self->options};
	my @table;
	while(@options) {
		my $spec = shift @options;
		shift @options if reftype($options[0]);
		$spec =~ s/\0(.*)$//;
		my $help = $1 // '';
		$spec =~ s/([!+:=]).*$//;
		my $args = $1 // '';
		my @names = split(/\|/, $spec);
		my ($long) = (grep(sub { length($_) > 1 }, @names), grep(sub { length($_) <= 1 }, @names));

		my @specs;
		foreach(@names) {
			my $spec;
			if(length($_) > 1) {
				$spec = "--$_";
				$spec .= "=..." if $args eq '=';
				$spec .= "[=...]" if $args eq ':';
				$spec .= " (--no-$long)" if $args eq '!';
			} else {
				$spec = "-$_";
				$spec .= " ..." if $args eq '=';
				$spec .= " [...]" if $args eq ':';
				$spec .= " (--no-$long)" if $args eq '!';
			}
			push @specs, $spec;
		}

		push @table, [join(', ', @specs), $help];
	}

	die Fruitbak::Command::List->format_table(\@table);
}

sub run {
	local $SIG{__DIE__} = sub {
		local $_ = shift;
		die $_ if ref $_;
		if(s/^.*\K at \S+ line \d+\.?\n\z//) {
			local $Carp::CarpLevel = 1;
			confess($_);
		} else {
			die $_;
		}
	};

	my $info = $commands{$_[0] // 'help'};

	unless($info) {
		warn "Unknown command '$_[0]'\n";
		$info = $commands{help};
	}

	my ($class) = @$info;

	my $cmd = $class->new(fbak => $self->fbak);

	my @options = @{$self->options};
	my @longopts;
	while(@options) {
		my $spec = shift @options;
		$spec =~ s/\0.*$//;
		my $dest;
		if(reftype($options[0])) {
			$dest = shift @options;
		} else {
			my ($name) = split /\W+/, $spec;
			my $sub = "opt_$name";
			$dest = sub { $cmd->$sub(@_) };
		}
		push @longopts, $spec, $dest;
	}
	return !GetOptions(@longopts) || $cmd->run(@_);
}

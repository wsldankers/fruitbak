=encoding utf8

=head1 NAME

Fruitbak::Command - implementation of CLI commands

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

package Fruitbak::Command;

use Class::Clarity -self;

use Scalar::Util qw(reftype);
use Getopt::Long qw(GetOptionsFromArray);

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
		['h|help|usage', undef, "Display usage information for this command"],
	]
}

sub opt_h {
	my $options = $self->options;
	my @table;
	foreach my $option (@$options) {
		my ($spec, undef, $desc) = @$option;
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

		push @table, [join(', ', @specs), $desc];
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

	my $options = $cmd->options;
	my @longopts;
	foreach my $option (@$options) {
		my ($spec, $dest, $desc) = @$option;
		unless(reftype($dest)) {
			my $method = $dest // do {
				my ($name) = split /\W+/, $spec;
				"opt_$name"
			};
			$dest = sub { my $val = pop; $cmd->$method($val, @_) };
		}
		push @longopts, $spec, $dest;
	}
	return !GetOptionsFromArray(\@_, @longopts) || $cmd->run(@_);
}

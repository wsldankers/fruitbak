"""Some hacks to get optional command arguments in click (a python CLI
option parsing toolkit). To use these, declare separate options ``foo`` and
``foo_set``, with corresponding arguments::

	@cli.command(cls = OptionalCommand)
	@click.option('--foo', cls = OptionalWithoutArgument, is_flag = True,
		help = "Activate foo")
	@click.option('--foo_set', cls = OptionalWithArgument,
		help = "Activate foo with this value")
	def frobber(foo, foo_set):
		...

The corresponding click command function should have both `foo` and
`foo_set` arguments, of which `foo` will be set to `True` or `False`
depending on whether ``--foo`` was present and `foo_set` will be set
to the supplied parameter or `None` of no parameter was present."""

import click
from itertools import chain

class OptionalWithoutArgument(click.Option):
	"""Mark this boolean option as getting a _set option"""
	optional = True

class OptionalWithArgument(click.Option):
	"""Mark this option as being the _set part of an option with an
	optional argument."""
	def get_help_record(self, ctx):
		"""Fix the help text for the _set suffix."""
		help = super().get_help_record(ctx)
		return (help[0].replace('_set ', '=', 1), *help[1:])

class OptionalCommand(click.Command):
	"""Command class for use with commands that have one or more options
	with optional arguments."""
	def parse_args(self, ctx, args):
		"""Translate any opt= to opt_set= as needed."""
		options = (o.opts for o in ctx.command.params if getattr(o, 'optional', None))
		prefixes = {p for p in chain(*options) if p.startswith('--')}
		newargs = []
		for arg in args:
			a = arg.split('=', maxsplit = 1)
			if len(a) > 1 and a[0] in prefixes:
				orig = a[0]
				a[0] = orig + '_set'
				newargs.append(orig)
				newargs.append('='.join(a))
			else:
				newargs.append(arg)

		return super().parse_args(ctx, newargs)

import click
from itertools import chain

class OptionalWithoutArgument(click.Option):
	"""Mark this option as getting a _set option"""
	optional = True

class OptionalWithArgument(click.Option):
	def get_help_record(self, ctx):
		"""Fix the help text for the _set suffix"""
		help = super().get_help_record(ctx)
		return (help[0].replace('_set ', '=', 1), *help[1:])

class OptionalCommand(click.Command):
	def parse_args(self, ctx, args):
		"""Translate any opt= to opt_set= as needed"""
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

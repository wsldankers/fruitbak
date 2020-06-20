"""Functions for dealing with process environments."""

from os import fsencode

def convert_env(env):
	"""Convert an environment mapping to something that can be passed to
	`subprocess.run()`. Keys and values are converted to bytes using
	`fsencode()` and any `None` values are treated as deletion markers and
	skipped.

	:param dict env: The mapping to convert.
	:return: The (newly created) converted mapping.
	:rtype: dict"""

	result = {}
	if env is not None:
		for k, v in env.items():
			if v is None:
				continue
			k = fsencode(k)
			if k in result:
				continue
			result[k] = fsencode(v)
	return result

def merge_env(base, *envs):
	"""Merge one or more mappings into one. Every argument
	is taken to be a mapping, mappings are iterated over starting
	with the first argument, entries from later mappings overwrite
	earlier ones. Each mapping is converted using `convert_env()`
	before processing (important for key equality).

	A `None` value in a mapping indicates deletion. So something like::

		merge_env({'a': 'x'}, {'a': None})

	would result in an empty return mapping.

	:param dict base: The first mapping to merge.
	:param \\*envs: Zero or more mappings to merge.
	:return: The The (newly created) merged mapping.
	:rtype: dict"""

	result = convert_env(base)
	for env in envs:
		for k, v in convert_env(env).items():
			if v is None:
				try:
					del result[k]
				except KeyError:
					pass
			else:
				result[k] = v
	return result

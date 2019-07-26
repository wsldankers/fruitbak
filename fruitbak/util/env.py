"""Functions for dealing with process environments."""

from os import fsencode

def convert_env(env):
	result = {}
	if env is not None:
		for k, v in env.items():
			k = fsencode(k)
			if k in result:
				continue
			try:
				v = fsencode(v)
			except:
				pass
			else:
				result[k] = v
	return result

def merge_env(base, *envs):
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

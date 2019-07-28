from pathlib import Path
from subprocess import run as subprocess_run, PIPE
from weakref import ref as weakref
from os import environ
from threading import local
from collections import ChainMap
from functools import wraps

from fruitbak.util import initializer, opener, ensure_bytes, ensure_str, merge_env, convert_env

class _configurable:
	def __init__(self, initializer):
		self._initializer = initializer
		self.__doc__ = initializer.__doc__

	def __get__(self, obj, objtype = None):
		initializer = self._initializer
		name = initializer.__name__

		try:
			config = obj.config
		except AttributeError:
			if obj is None:
				# This typically happens when querying for docstrings,
				# so return something with the appropriate docstring.
				return self
			raise

		try:
			value = config[name]
		except KeyError:
			value = initializer(obj)
		else:
			value = self._validate(obj, objtype, value)

		return self._prepare(obj, objtype, value)

	def _validate(self, obj, objtype, value):
		return self._validator(obj, value)

	def _prepare(self, obj, objtype, value):
		return self._preparator(obj, value)

	# staticmethod because this isn't a method for the property object itself
	@staticmethod
	def _validator(self, value):
		return value

	# staticmethod because this isn't a method for the property object itself
	@staticmethod
	def _preparator(self, value):
		return value

	def validate(self, f):
		self._validator = f
		return self

	def prepare(self, f):
		self._preparator = f
		return self

class configurable(_configurable):
	def __get__(self, obj, objtype = None):
		value = super(configurable, self).__get__(obj, objtype)
		setattr(obj, self._initializer.__name__, value)
		return value

class configurable_function(configurable):
	def __init__(self, initializer):
		@wraps(initializer)
		def _initializer(self):
			return initializer
		super().__init__(_initializer)

class configurable_property(_configurable):
	def _validate(self, obj, objtype, value):
		value = super()._validate(obj, objtype, value)
		return value(obj)

class configurable_command(configurable):
	def _prepare(self, obj, objtype, value):
		value = super()._prepare(obj, objtype, value)
		if callable(value):
			return value
		config = self.config
		def command(*args, **kwargs):
			subprocess_run((b'/bin/sh', b'-ec', ensure_bytes(value), 'sh'), env = config.env)
		return command

class delayed:
	def __init__(self, f):
		self.f = f

	def __call__(self, *args, **kwargs):
		return self.f(*args, **kwargs)

# Ugly: the official way is to use the builtins module, but that is
# not a dict we can extend. However, exec() is documented to populate
# the __builtins__ key of globals with something that is usable.
#builtins = {}
#exec('', builtins)
#builtins = builtins['__builtins__']
import builtins as builtins_module
builtins = vars(builtins_module)

# FIXME: environment related stuff needs to be moved to a separate module
class ConfigEnvironment:
	def __init__(self, config, *args, **kwargs):
		self.tls = config.tls
		self.history = []
		env = {}
		for a in args:
			env.update(a)
		env.update(kwargs)
		self.env = env

	def __enter__(self):
		tls = self.tls
		try:
			oldenv = tls.env
		except AttributeError:
			oldenv = {}
			tls.env = oldenv
			env = dict(self.env)
		else:
			env = dict(oldenv)
			env.update(self.env)
			self.history.append(oldenv)
		tls.env = env

	def __exit__(self, exc_type, exc_value, traceback):
		history = self.history
		tls = self.tls
		if history:
			tls.env = self.history.pop()
		else:
			del tls.env

class subdict(dict):
	"""Subclass dict so that we can weakref it"""

class Config:
	def __init__(self, *paths, dir_fd = None, env = None, preseed = None):
		# thread-local storage
		tls = local()
		self.tls = tls

		if preseed is None:
			globals = subdict()
		else:
			globals = subdict(preseed)
		self.globals = globals

		extra_builtins = dict(builtins)
		extra_builtins['delayed'] = delayed
		globals['__builtins__'] = extra_builtins
		weak_globals = weakref(globals)

		def include(path):
			with open(str(path) + '.py', opener = opener(dir_fd = dir_fd)) as f:
				content = f.read()
			exec(content, weak_globals())
		extra_builtins['include'] = include

		cfg_env = convert_env(env)
		self.cfg_env = cfg_env

		def run(*args, env = None, **kwargs):
			tls_env = getattr(tls, 'env', {})
			env = merge_env(environ, cfg_env, tls_env, env)
			kwargs = dict(kwargs, env = new_env)
			return subprocess_run(*args, **kwargs)
		extra_builtins['run'] = run

		def backticks(command, *args, **kwargs):
			if kwargs.get('shell') is None:
				try:
					command = (b'/bin/sh', b'-ec', ensure_bytes(command), b'sh', *args)
				except TypeError:
					command = (*command, *args)
				else:
					kwargs['shell'] = False
			completed = run(command, stdout = PIPE, **kwargs)
			completed.check_returncode()
			return ensure_str(completed.stdout)
		extra_builtins['backticks'] = backticks

		for p in paths:
			include(p)

	def __getitem__(self, key):
		value = self.globals[key]
		while isinstance(value, delayed):
			value = value(self)
			#self.globals[key] = value
		return value

	def __contains__(self, key):
		return key in self.globals

	def get(self, key, default = None):
		value = self.globals.get(key, default)
		while isinstance(value, delayed):
			value = value(self)
			#self.globals[key] = value
		return value

	def copy(self):
		dup = type(self)()
		dup.tls = self.tls
		dup.globals = ChainMap({}, self.globals)
		return dup

	def update(self, *args, **kwargs):
		self.globals.update(*args, **kwargs)

	def __repr__(self):
		rep = ["[config for %s]\n" % self.globals['name']]
		for key, value in self.globals.items():
			if isinstance(value, delayed):
				rep.append(key + " = (delayed)\n")
			else:
				rep.append("%s = %s\n" % (key, repr(value)))
		return "".join(rep)

	def setenv(self, *args, **kwargs):
		return ConfigEnvironment(self, *args, **kwargs)

	@property
	def env(self):
		return merge_env(environ, self.cfg_env, getattr(self.tls, 'env', {}))

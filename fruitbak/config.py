from pathlib import Path
from subprocess import run as subprocess_run
from weakref import ref as weakref
from os import environ, fsencode, open as os_open, O_CLOEXEC, O_NOCTTY
from threading import local

from fruitbak.util.clarity import initializer

class configurable:
	def __init__(self, initializer):
		self._initializer = initializer
		self.__doc__ = initializer.__doc__

	def __get__(self, obj, objtype = None):
		initializer = self._initializer
		name = initializer.__name__
		config = obj.config
		try:
			value = config[name]
		except KeyError:
			value = initializer(obj)
		else:
			value = self._validate(obj, value)
		value = self._prepare(obj, value)
		setattr(obj, name, value)
		return value

	# staticmethod because this isn't a method for the property object
	@staticmethod
	def _validate(self, value):
		return value

	# staticmethod because this isn't a method for the property object
	@staticmethod
	def _prepare(self, value):
		return value

	def validate(self, f):
		self._validate = f
		return self

	def prepare(self, f):
		self._prepare = f
		return self

class configurable_function(configurable):
	def __init__(self, initializer):
		def _initializer(self):
			return initializer
		_initializer.__name__ = initializer.__name__
		_initializer.__doc__ = initializer.__doc__
		super().__init__(_initializer)

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
	def __init__(self, path, *paths, dir_fd = None, preseed = None):
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

		def opener(file, flags):
			return os_open(file, flags|O_CLOEXEC|O_NOCTTY, dir_fd = dir_fd)

		def include(path):
			with open(str(path) + '.py', opener = opener) as f:
				content = f.read()
			exec(content, weak_globals())
		extra_builtins['include'] = include

		def run(*args, env = None, **kwargs):
			try:
				tlsenv = tls.env
			except AttributeError:
				pass
			else:
				newenv = {}
				if env is None:
					for k, v in environ.items():
						newenv[fsencode(k)] = fsencode(v)
				else:
					for k, v in env.items():
						newenv[fsencode(k)] = fsencode(v)

				for k, v in tlsenv.items():
					k = fsencode(k)
					if k in newenv:
						continue
					try:
						v = fsencode(v)
					except:
						pass
					else:
						newenv[k] = v

				kwargs = dict(kwargs, env = newenv)
			return subprocess_run(*args, **kwargs)
		extra_builtins['run'] = run

		include(path)
		for p in paths:
			include(p)

	def __getitem__(self, key):
		value = self.globals[key]
		while isinstance(value, delayed):
			value = value()
			self.globals[key] = value
		return value

	def __contains__(self, key):
		return key in self.globals

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
		return self.tls.env

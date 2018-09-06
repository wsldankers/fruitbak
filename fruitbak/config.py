from pathlib import Path
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
		return super().__init__(_initializer)

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

class Config:
	def __init__(self, basepath, path, **kwargs):
		basepath = Path(basepath)

		globals = dict(kwargs)
		self.globals = globals

		extra_builtins = dict(builtins)
		extra_builtins['delayed'] = delayed
		globals['__builtins__'] = extra_builtins

		try:
			def include(path):
				with open(str(basepath / path) + '.py') as f:
					content = f.read()
				exec(content, globals)

			extra_builtins['include'] = include

			include(path)
		finally:
			# break reference loops
			globals = None
			del extra_builtins['include']

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

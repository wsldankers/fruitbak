import weakref
from fruitbak.util.clarity import initializer

class weakproperty(property):
	def __init__(self, f):
		dict = self.__dict__
		name = f.__name__
		def getter():
			return dict[name]()

		def deleter():
			del dict[name]

		def setter(self, value):
			dict[name] = weakref.ref(value, deleter)

		super().__init__(getter, setter, deleter)

class pouch:
	def __init__(self, **kwargs):
		self.__dict__.update(kwargs)

class multicall(list):
	def __call__(self, *args, **kwargs):
		exc = None
		for f in self:
			try:
				f(*args, **kwargs)
			except Exception as e:
				exc = e
		if e is not None:
			raise e

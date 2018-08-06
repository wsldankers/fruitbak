from fruitbak.util.clarity import initializer

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

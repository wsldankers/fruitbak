from fruitbak.util.clarity import Clarity, stub, initializer
from fruitbak.util.weak import weakproperty

class Handler(Clarity):
	@weakproperty
	def fruitbak(self):
		return self.pool.fruitbak

	@weakproperty
	def pool(self):
		raise RuntimeError("%.pool used uninitialized" % type(self).__name__)

	@stub
	def get_chunk(self, callback):
		pass

	@stub
	def put_chunk(self, callback, hash, value):
		pass

	@stub
	def del_chunk(self, callback, hash):
		pass

class Filter(Handler):
	def __init__(self, subordinate, **kwargs):
		super().__init__(**kwargs)
		self.subordinate = subordinate

	def get_chunk(self, callback, hash):
		return self.subordinate.get_chunk(callback, hash)

	def put_chunk(self, callback, hash, value):
		return self.subordinate.put_chunk(callback, hash, value)

	def del_chunk(self, callback, hash):
		return self.subordinate.del_chunk(callback, hash)

class Storage(Handler):
	pass

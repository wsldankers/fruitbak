from fruitbak.util.clarity import Clarity, stub, initializer
from fruitbak.util.locking import locked
from fruitbak.util.weak import weakproperty

from threading import RLock
from concurrent.futures import ThreadPoolExecutor

class Handler(Clarity):
	def __init__(self, *args, **kwargs):
		self.lock = RLock()
		return super().__init__(*args, **kwargs)

	@weakproperty
	def fruitbak(self):
		return self.pool.fruitbak

	@weakproperty
	def pool(self):
		raise RuntimeError("%.pool used uninitialized" % type(self).__name__)

	max_workers = 32

	@locked
	@initializer
	def executor(self):
		return ThreadPoolExecutor(max_workers = self.max_workers)

	@stub
	def has_chunk(self, callback, hash):
		pass

	@stub
	def get_chunk(self, callback, hash):
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

	def has_chunk(self, callback, hash):
		return self.subordinate.has_chunk(callback, hash)

	def get_chunk(self, callback, hash):
		return self.subordinate.get_chunk(callback, hash)

	def put_chunk(self, callback, hash, value):
		return self.subordinate.put_chunk(callback, hash, value)

	def del_chunk(self, callback, hash):
		return self.subordinate.del_chunk(callback, hash)

class Storage(Handler):
	pass

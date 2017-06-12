from fruitbak.util.clarity import Clarity, stub

class Handler(Clarity):
	@stub
	def get_chunk(self, hash, callback):
		pass

	@stub
	def put_chunk(self, hash, value, callback):
		pass

	@stub
	def del_chunk(self, hash, callback):
		pass

class Filter(Handler):
	def __init__(self, subordinate, **kwargs):
		super().__init__(**kwargs)
		self.subordinate = subordinate

	def get_chunk(self, hash, callback):
		return self.subordinate.get_chunk(hash, callback)

	def put_chunk(self, hash, value, callback):
		return self.subordinate.put_chunk(hash, value, callback)

	def del_chunk(self, hash, callback):
		return self.subordinate.del_chunk(hash, callback)

class Storage(Handler):
	pass

from fruitbak.util.clarity import Clarity, stub

class Handler(Clarity):
	@stub
	def get_chunk(self, hash):
		pass

	@stub
	def put_chunk(self, hash, value):
		pass

	@stub
	def del_chunk(self, hash):
		pass

class Filter(Handler):
	def __init__(self, subordinate, **kwargs):
		super().__init__(**kwargs)
		self.subordinate = subordinate

	def get_chunk(self, hash):
		return self.subordinate.get_chunk(hash)

	def put_chunk(self, hash, value):
		return self.subordinate.put_chunk(hash, value)

	def del_chunk(self, hash):
		return self.subordinate.del_chunk(hash)

class Storage(Handler):
	pass

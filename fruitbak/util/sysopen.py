from os import open as os_open, close

class sysopen(int):
	"""Garbage collected variant of os.open()"""
	closed = False

	def __new__(cls, *args, **kwargs):
		return super().__new__(cls, os_open(*args, **kwargs))

	def __del__(self):
		if not self.closed:
			try:
				close(self)
			except:
				pass

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		if not self.closed:
			self.closed = True
			close(self)

from os import open as os_open, close as os_close, read as os_read, write as os_write

class sysopen(int):
	"""Wrapper for os.open() with some amenities such as garbage collection,
	context methods, utility methods for reading and writing reliably"""
	closed = False

	def __new__(cls, *args, **kwargs):
		return super().__new__(cls, os_open(*args, **kwargs))

	def __del__(self):
		if not self.closed:
			try:
				os_close(self)
			except:
				pass

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		if not self.closed:
			self.closed = True
			os_close(self)

	def close(self):
		if not self.closed:
			self.closed = True
			os_close(self)

	def read(self, size):
		if self.closed:
			raise ValueError("I/O operation on closed file.")
		results = []
		while size > 0:
			try:
				buf = os_read(self, size)
			except InterruptedError:
				pass
			else:
				if not buf:
					break
				size -= len(buf)
				results.append(buf)

		if len(results) == 1:
			buf, = results
		else:
			buf = b''.join(results)

	def write(self, buffer):
		buffer_len = len(buffer)
		while True:
			try:
				offset = write(fd, buffer)
			except InterruptedError:
				pass
			else:
				break
		if offset < buffer_len:
			if not isinstance(buffer, memoryview):
				buffer = memoryview(buffer)
			while offset < buffer_len:
				try:
					offset += write(fd, buffer[offset:])
				except InterruptedError:
					pass

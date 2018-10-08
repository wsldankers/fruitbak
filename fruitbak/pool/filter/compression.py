from lzma import compress, decompress, FORMAT_RAW, FILTER_LZMA2
from sys import exc_info

from fruitbak.util import initializer
from fruitbak.pool.handler import Filter

class XZ(Filter):
	@initializer
	def preset(self):
		return None

	@initializer
	def filters(self):
		compressor = {'id': FILTER_LZMA2}
		preset = self.preset
		if preset is not None:
			compressor['preset'] = preset
		return (compressor,)

	def get_chunk(self, callback, hash):
		filters = self.filters
		cpu_executor = self.cpu_executor

		def when_done(value, exception):
			if value is None:
				callback(value, exception)
			else:
				def unxz():
					try:
						d = decompress(value, format = FORMAT_RAW, filters = filters)
					except:
						callback(None, exc_info())
					else:
						callback(d, exception)
				cpu_executor.submit(unxz)

		return super().get_chunk(when_done, hash)

	def put_chunk(self, callback, hash, value):
		filters = self.filters
		cpu_executor = self.cpu_executor
		subordinate = self.subordinate

		def xz():
			try:
				c = compress(value, format = FORMAT_RAW, filters = filters)
			except:
				callback(None, exc_info())
			else:
				subordinate.put_chunk(callback, hash, c)

		cpu_executor.submit(xz)

from sys import exc_info

from fruitbak.util import initializer
from fruitbak.pool.handler import Filter

class Compressor(Filter):
	@initializer
	def preset(self):
		return None

	def get_chunk(self, callback, hash):
		cpu_executor = self.cpu_executor

		def when_done(value, exception):
			if value is None:
				callback(value, exception)
			else:
				decompress = self.decompress
				def job():
					try:
						d = decompress(value)
					except:
						callback(None, exc_info())
					else:
						callback(d, exception)
				cpu_executor.submit(job)

		return super().get_chunk(when_done, hash)

	def put_chunk(self, callback, hash, value):
		cpu_executor = self.cpu_executor
		subordinate = self.subordinate
		compress = self.compress

		def job():
			try:
				c = compress(value)
			except:
				callback(None, exc_info())
			else:
				subordinate.put_chunk(callback, hash, c)

		cpu_executor.submit(job)

import zlib
class Gzip(Compressor):
	@initializer
	def compress(self):
		preset = self.preset
		if preset is None:
			return zlib.compress
		else:
			return lambda value: zlib.compress(value, preset)

	decompress = zlib.decompress

try:
	import lzma
except ImportError:
	pass
else:
	class XZ(Compressor):
		@initializer
		def filters(self):
			compressor = {'id': lzma.FILTER_LZMA2}
			preset = self.preset
			if preset is not None:
				compressor['preset'] = preset
			return (compressor,)

		@initializer
		def compress(self):
			filters = self.filters
			return lambda value: lzma.compress(value, format = lzma.FORMAT_RAW, filters = filters)

		@initializer
		def decompress(self):
			filters = self.filters
			return lambda value: lzma.decompress(value, format = lzma.FORMAT_RAW, filters = filters)

try:
	import lz4
except ImportError:
	pass
else:
	class LZ4(Compressor):
		@initializer
		def compress(self):
			return lz4.compressHC if self.preset else lz4.compress

		decompress = lz4.decompress

try:
	import lz4.block
except ImportError:
	pass
else:
	class LZ4Block(Compressor):
		@initializer
		def compress(self):
			preset = self.preset
			if preset < 0:
				return lambda value: lz4.block.compress(value, mode = 'fast', acceleration = -preset)
			elif preset > 0:
				return lambda value: lz4.block.compress(value, mode = 'high_compression', compression = preset)
			else:
				return lz4.block.compress

		decompress = lz4.block.decompress

try:
	import lz4.frame
except ImportError:
	pass
else:
	class LZ4Frame(Compressor):
		@initializer
		def preset(self):
			return 0

		@initializer
		def block_size(self):
			return lz4.frame.BLOCKSIZE_MAX4MB

		@initializer
		def compress(self):
			preset = self.preset
			block_size = self.block_size
			return lambda value: lz4.frame.compress(value, block_size = block_size, compression_level = preset, store_size = False)

		decompress = lz4.frame.decompress

try:
	import snappy
except ImportError:
	pass
else:
	class Snappy(Compressor):
		compress = snappy.compress
		decompress = snappy.decompress

try:
	import brotli
except ImportError:
	pass
else:
	class Brotli(Compressor):
		@initializer
		def compress(self):
			preset = self.preset
			if preset is None:
				return lambda value: brotli.compress(value, lgblock = 24)
			else:
				return lambda value: brotli.compress(value, lgblock = 24, quality = preset)

		decompress = brotli.decompress

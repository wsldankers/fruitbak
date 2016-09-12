from fruitbak.util.clarity import Clarity, initializer
from fruitbak.pool.handler import Storage
from base64 import b64encode
from os import fsencode, fsdecode

class Filesystem(Storage):
	@initializer
	def base_path(self):
		return self.cfg['base_path']

	def get_chunk(self, hash):
		b64 = b64encode(hash, b'+_')
		b64 = b64.rstrip(b'=')
		relpath = b'/' + b64[:2] + b'/' + b64[2:]
		base_path = self.base_path
		if isinstance(base_path, str):
			relpath = fsdecode(relpath)
		path = base_path + relpath
		with open(path, mode = 'rb', buffering = 0) as f:
			return f.read()

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.pool.handler import Storage

from weakref import ref as weakref
from base64 import b64encode
from traceback import format_exc
from sys import stderr
from concurrent.futures import ThreadPoolExecutor
from tempfile import NamedTemporaryFile
from os import mkdir, replace, fsync, unlink, fsdecode
from pathlib import Path

def windshield(f, *args, **kwargs):
	try:
		f(*args, **kwargs)
	except:
		print(format_exc(), file = stderr)

class Filesystem(Storage):
	@initializer
	def pooldir(self):
		return self.config['pooldir']

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self.executor = ThreadPoolExecutor(max_workers = 32)

	def hash2path(self, hash):
		b64 = b64encode(hash, b'+_').rstrip(b'=')
		return self.pooldir / Path(fsdecode(b64[:2])) / Path(fsdecode(b64[2:]))

	def hash2parent(self, hash):
		b64 = b64encode(hash, b'+_').rstrip(b'=')
		return self.pooldir / Path(fsdecode(b64[:2]))

	def get_chunk(self, hash, callback):
		path = self.hash2path(hash)

		def job():
			try:
				with path.open(mode = 'rb', buffering = 0) as f:
					buf = f.read()
				callback(buf, None)
			except Exception as e:
				callback(None, e)

		self.executor.submit(windshield, job)

	def put_chunk(self, hash, value, callback):
		path = self.hash2path(hash)

		def job():
			try:
				with NamedTemporaryFile(dir = str(self.pooldir), buffering = 0, delete = False) as f:
					f_path = Path(f.name)
					try:
						offset = f.write(value)
						if offset < len(value):
							m = memoryview(value)
							while offset < len(value):
								offset += f.write(m[offset:])
						f.flush()
						fsync(f.fileno())
						f.close()
						try:
							f_path.replace(path)
						except FileNotFoundError:
							self.hash2parent(hash).mkdir()
							f_path.replace(path)
					except:
						f_path.unlink()
						raise

				callback(None)
			except Exception as e:
				callback(e)

		self.executor.submit(windshield, job)

	def del_chunk(self, hash, callback):
		path = self.hash2path(hash)

		def job():
			try:
				path.unlink()
			except FileNotFoundError:
				callback(None)
			except Exception as e:
				callback(e)
			callback(None)

		self.executor.submit(windshield, job)

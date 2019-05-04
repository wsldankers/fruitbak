from sys import exc_info

from fruitbak.util import initializer, locked
from fruitbak.pool.handler import Filter

from base64 import b64encode, b64decode
from Crypto.Cipher import AES
import nacl.secret
import nacl.utils

class Encrypt(Filter):
	@initializer
	def key(self):
		return self.pool.pool_encryption_key

	@locked
	@initializer
	def validated_key(self):
		key = self.key
		if key is None:
			raise RuntimeError("No encryption key configured. Add this to your configuration:\npool_encryption_key = %r"
				% (b64encode(nacl.utils.random(32)).decode(),))
		if isinstance(key, str):
			key = b64decode(key)
		else:
			try:
				memoryview(key)
			except TypeError:
				raise TypeError("encryption key must be bytes-like, not %r" % (type(key).__name__,)) from None
			key = bytes(key)
		if len(key) != 32:
			raise TypeError("encryption key must be 32 bytes long, not %d" % (len(key),))
		return key

	@locked
	@initializer
	def aes(self):
		if self.fruitbak.hash_size % AES.block_size:
			raise RuntimeError("size of hash function (%d) is incompatible with AES block size (%d)"
				% (self.fruitbak.hash_size, AES.block_size))
		# we get away with reusing this because the mode is ECB
		return AES.new(self.validated_key)

	@locked
	@initializer
	def box(self):
		return nacl.secret.SecretBox(self.validated_key)

	@locked
	@initializer
	def encrypt(self):
		box = self.box
		nonce = nacl.utils.random(box.NONCE_SIZE)
		return lambda value: box.encrypt(value, nonce)

	@initializer
	def decrypt(self):
		return self.box.decrypt

	def get_chunk(self, callback, hash):
		cpu_executor = self.cpu_executor

		def when_done(value, exception):
			if value is None:
				callback(value, exception)
			else:
				decrypt = self.decrypt
				def job():
					try:
						d = decrypt(value)
					except:
						callback(None, exc_info())
					else:
						callback(d, exception)
				cpu_executor.submit(job)

		return super().get_chunk(when_done, self.aes.encrypt(bytes(hash)))

	def put_chunk(self, callback, hash, value):
		cpu_executor = self.cpu_executor
		subordinate = self.subordinate
		encrypt = self.encrypt

		hash = self.aes.encrypt(bytes(hash))

		def job():
			try:
				c = encrypt(value)
			except:
				callback(exc_info())
			else:
				subordinate.put_chunk(callback, hash, c)

		cpu_executor.submit(job)

	def lister(self, agent):
		decrypt = self.aes.decrypt
		for hash in self.subordinate.lister(agent):
			yield decrypt(hash)

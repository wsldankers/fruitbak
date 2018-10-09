from sys import exc_info

from fruitbak.util import initializer
from fruitbak.pool.handler import Filter

from Crypto.Cipher import AES
import nacl.secret
import nacl.utils

class Encrypt(Filter):
	@initializer
	def key(self):
		return b'01234567890123456789012345678901'

	@initializer
	def aes(self):
		# we get away with reusing this because the mode is ECB
		if self.fruitbak.hashsize % AES.block_size:
			raise RuntimeError("size of hash function (%d) is incompatible with AES block size (%d)"
				% (self.fruitbak.hashsize, AES.block_size))
		return AES.new(self.key)

	@initializer
	def box(self):
		return nacl.secret.SecretBox(self.key)

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

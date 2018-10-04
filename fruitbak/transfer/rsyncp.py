class LocalTransfer(Clarity):
	@initializer
	def fruitbak(self):
		return self.newshare.fruitbak

	@initializer
	def path(self):
		return self.newshare.path

	@initializer
	def reference(self):
		return self.newshare.reference

	def transfer(self):
		newshare = self.newshare
		reference = self.reference
		chunksize = self.fruitbak.chunksize



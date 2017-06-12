from fruitbak.util.clarity import Clarity
from fruitbak.pool.filesystem import Filesystem

from time import sleep

class Cli(Clarity):
	def run(self):
		if self.argv[1] == 'test':
			fs = Filesystem(cfg = {'base_path': '/tmp/foo'})
			def done(hash, content):
				print('%s: %s' % (hash, content))
			for x in range(1000):
				fs.get_chunk(str(x).encode(), done)
			sleep(3)
		else:
			raise Exception("unknown command")

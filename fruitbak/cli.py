from fruitbak.util.clarity import Clarity
from fruitbak.pool import Pool
from fruitbak.pool.filesystem import Filesystem

from time import sleep

class Cli(Clarity):
	def run(self):
		if self.argv[1] == 'testfs':
			fs = Filesystem(cfg = {'base_path': '/tmp/foo'})
			def done(hash, content):
				print('%s: %s' % (hash, content))
			for x in range(10):
				fs.get_chunk(str(x).encode(), done)
			sleep(1)
		elif self.argv[1] == 'test':
			pool = Pool()
			agent = pool.agent()
			while agent.wait() is not None:
				pass
		else:
			raise Exception("unknown command")

from fruitbak.util.clarity import Clarity
from fruitbak.pool import Pool
from fruitbak.pool.filesystem import Filesystem

from time import sleep

class Cli(Clarity):
	def run(self):
		if self.argv[1] == 'testfs':
			fs = Filesystem(cfg = self.cfg)
			def done(hash, content):
				print('%s: %s' % (hash, content))
			for x in range(10):
				fs.get_chunk(str(x).encode(), done)
			sleep(1)
		elif self.argv[1] == 'test':
			pool = Pool(cfg = self.cfg)
			agent = pool.agent()
			agent.queue_read(b'UUUUUUUUUUUUUUUUUUUUUUUUUUU')
			agent.queue_read(b'XUUUUUUUUUUUUUUUUUUUUUUUUUU')
			agent.queue_write(b'YUUUUUUUUUUUUUUUUUUUUUUUUUU', b'wuyertowyueru')
			res = agent.wait()
			print(res['value'])
			res = agent.wait()
			print(res['value'])
			res = agent.wait()
			print(res['value'])
		else:
			raise Exception("unknown command")

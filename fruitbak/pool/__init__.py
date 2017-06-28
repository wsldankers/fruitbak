from weakref import ref as weakref

from warnings import warn
from threading import Condition, RLock
from collections import deque

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.heapmap import MinHeapMap
from fruitbak.pool.filesystem import Filesystem

class mapnode(weakref):
	def __init__(self, agent, *args):
		super().__init__(agent, *args)
		self.hash = hash(agent)
		self.id = id(agent)

	def __hash__(self):
		return self.hash

	def __eq__(self, other):
		return self.id == other.id

class PoolAgent(Clarity):
	def dequeue(self):
		f = self.queue.popleft()
		f(self)

	@initializer
	def cond(self):
		return Condition(self.pool.lock)

	@initializer
	def queue(self):
		return deque()

	@initializer
	def done(self):
		return []

	@initializer
	def pending(self):
		return set()

	@property
	def avarice(self):
		return len(self.done) + len(self.pending)

	def update_registration(self):
		if self.queue:
			self.pool.register_agent(self)
		else:
			self.pool.unregister_agent(self)

	def queue_read(self, hash):
		pool = self.pool
		def read_operation(self):
			self.pending.add(hash)
			self.update_registration()
			def whendone(hash, value):
				with self.cond:
					self.pending.remove(hash)
					self.done.append({'hash': hash, 'value': value})
					self.update_registration()
					self.cond.notify()
					pool.replenish_queue()
			pool.root.get_chunk(hash, whendone)
			
		with self.cond:
			self.queue.append(read_operation)
			self.update_registration()
			pool.replenish_queue()

	def wait(self):
		with self.cond:
			while (self.queue or self.pending) and not self.done:
				self.cond.wait()
			try:
				return self.done.pop()
			except KeyError:
				return None

class Pool(Clarity):
	def __init__(self, *args, **kwargs):
		super().__init__(lock = RLock(), *args, **kwargs)

	@initializer
	def max_queue_depth(self):
		return 32

	@initializer
	def queue_depth(self):
		return 0

	@initializer
	def root(self):
		return Filesystem(cfg = self.cfg)

	@initializer
	def agents(self):
		return MinHeapMap()

	def agent(self, *args, **kwargs):
		a = PoolAgent(pool = self, *args, **kwargs)
		self.register_agent(a)
		return a

	def select_most_modest_agent(self):
		agents = self.agents
		agentref = agents.peek()
		return agentref()

	def register_agent(self, agent):
		agents = self.agents
		try:
			del agents[mapnode(agent)]
		except KeyError:
			pass
		node = None
		def finalizer(r):
			del agents[node]
		node = mapnode(agent, finalizer)
		agents[node] = agent.avarice

	def unregister_agent(self, agent):
		del self.agents[mapnode(agent)]

	def replenish_queue(self):
		while len(self.agents.heap) and self.queue_depth < self.max_queue_depth:
			agent = self.select_most_modest_agent()
			if agent is None:
				break
			agent.dequeue()

from weakref import ref as weakref

from warnings import warn
from threading import Condition, RLock

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.util.heapmap import MinHeapMap

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
	def __next__(self):
		x = self.queued.popleft()
		self.pending.add(x)
		self.pool.register(self)
		def whendone():
			self.pending.remove(x)
			self.done.push(x)
		return whendone

	@initializer
	def cond(self):
		return Condition(self.pool.lock)

	@initializer
	def queued(self):
		return [3]

	@initializer
	def done(self):
		return []

	@initializer
	def pending(self):
		return set()

	value = property(lambda self: len(self.done) + len(self.pending))

	def wait(self):
		self.pool.wait(self)

class Pool(Clarity):
	def __init__(self, *args, **kwargs):
		super().__init__(lock = RLock(), agents = MinHeapMap(), max_queue_depth = 32, *args, **kwargs)

	@initializer
	def queue_depth(self):
		return 0

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
			warn(str(node.id))
			self.unregister_agent(node)
		node = mapnode(agent, finalizer)
		agents[node] = agent.value

	def unregister_agent(self, agent):
		warn("unregister")
		agents = self.agents
		try:
			del agents[mapnode(agent)]
		except KeyError:
			warn("KeyError")
			pass

	def wait(self, agent):
		with agent.cond:
			while agent.pending and not agent.done:
				while len(self.agents.heap) and self.queue_depth >= self.max_queue_depth:
					a = self.select_most_modest_agent()
					if a is None:
						break
					req = next(a)
					if req is None:
						warn("req is None")
						self.unregister_agent(a)
					else:
						def on_completion():
							a.done.push(req)
							a.cond.notify()
						self.root.queue(req, on_completion)

				while self.queue_depth >= self.max_queue_depth and not agent.done:
					self.cond.wait()

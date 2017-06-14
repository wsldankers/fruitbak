from weakref import ref as weakref

from fruitbak.util.clarity import Clarity, initializer

class mapnode(weakref):
	def __init__(self, agent):
		super().__init__(agent)
		self.hash = hash(agent)
		self.id = id(agent)

	def __hash__(self):
		return self.hash

	def __eq__(self, other):
		return self.id == other.id

	def value(self):
		agent = self()
		if agent is None:
			return self.lastvalue
		v = len(agent.done) + len(agent.pending)
		self.lastvalue = v
		return v

class Pool(Clarity):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

		self.agents = MinHeapMap()

	def agent(*args, **kwargs):
		a = Agent(*args, **kwargs)
		w = weakref(a)

	def select_most_modest_agent(self):
		agents = self.agents
		agentref = agents.peek()
		return agentref()
		agents[agent] = len(agent.done) + len(agent.pending)

	def wait(agent):
		self.agents.push(agent)
		with agent.cond:
			while not agent.done:
				while self.queue_depth >= self.max_queue_depth:
					a = self.select_most_modest_agent()
					req = a.next()

					def on_completion():
						a.done.push(req)
						a.cond.notify()

					self.root.queue(req, on_completion)
				while self.queue_depth >= self.max_queue_depth and not agent.done:
					self.cond.wait()
		self.agents.pop(agent)

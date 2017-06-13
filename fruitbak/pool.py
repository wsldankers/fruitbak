from weakref import ref as weakref

from fruitbak.util.clarity import Clarity, initializer

class mapnode:
	__slots__ = ('__call__', lastvalue)

	def __init__(self, agent):
		self.__call__ = weakref(agent)
		self.value = 0

	def value(self):
		agent = self()
		if agent is None:
			return self.lastvalue
		v = len(agent.done) + len(agent.pending)
		self.lastvalue = v
		return v

	def __gt__(self, other):
		return self.value() < other.value()

class Pool(Clarity):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

		def agentcmp(wa, wb):


		self.agents = MaxHeapMap(

	def agent(*args, **kwargs):
		a = Agent(*args, **kwargs)
		w = weakref(a)
		

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

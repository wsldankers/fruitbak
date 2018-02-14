from pathlib import Path

class delayed:
	def __init__(self, f):
		self.f = f

	def __call__(self, *args, **kwargs):
		return self.f(*args, **kwargs)

class Config:
	def __init__(self, filename, **kwargs):
		self.locals = {}

		def readfile(filename):
			def include(includefilename):
				readfile(filename.parent / includefilename)
			globals = dict(kwargs)
			globals['delayed'] = delayed
			globals['include'] = include
			with open(str(filename) + '.py') as f:
				content = f.read()
			exec(content, globals, self.locals)

		readfile(Path(filename))

	def __getitem__(self, key):
		value = self.locals[key]
		if isinstance(value, delayed):
			value = value()
			self.locals[key] = value
		return value

	def __repr__(self):
		rep = ["[config for %s]\n" % self.locals['name']]
		for key, value in self.locals.items():
			if isinstance(value, delayed):
				rep.append(key + " = (delayed)\n")
			else:
				rep.append("%s = %s\n" % (key, repr(value)))
		return "".join(rep)

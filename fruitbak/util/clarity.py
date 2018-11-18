from types import MethodType as method_closure

class getinitializer:
	def __init__(self, getfunction):
		self.getfunction = getfunction
		self.__doc__ = getfunction.__doc__

	def __get__(self, obj, objtype = None):
		getfunction = self.getfunction
		name = getfunction.__name__
		try:
			objdict = obj.__dict__
		except AttributeError:
			# This typically happens when querying for docstrings,
			# so return something with the appropriate docstring.
			return self
		# Do an explicit check to accommodate a threaded scenario:
		# even if this getter is protected by a lock, that lock is
		# not taken when python checks whether the attribute exists
		# in the __dict__. So when we run, another thread might
		# have caused the __dict__ entry to be populated.
		try:
			return objdict[name]
		except KeyError:
			pass
		value = getfunction(obj)
		objdict[name] = value
		return value

	def setter(self, setfunction):
		return getsetinitializer(self.getfunction, setfunction)

	def deleter(self, delfunction):
		return getdelinitializer(self.getfunction, delfunction)

class getdelinitializer(getinitializer):
	def __init__(self, getfunction, delfunction):
		super().__init__(getfunction)
		self.name = getfunction.__name__
		self.delfunction = delfunction

	def __set__(self, obj, value):
		obj.__dict__[self.name] = value

	def __delete__(self, obj):
		name = self.name
		objdict = obj.__dict__
		self.delfunction(obj)
		try:
			del objdict[name]
		except KeyError:
			pass
		raise AttributeError(name)

	def setter(self, setfunction):
		return getsetdelinitializer(self.getfunction, setfunction, self.delfunction)

class getsetinitializer(getinitializer):
	def __init__(self, getfunction, setfunction):
		super().__init__(getfunction)
		self.name = getfunction.__name__
		self.setfunction = setfunction

	def __set__(self, obj, value):
		obj.__dict__[self.name] = self.setfunction(obj, value)

	def __delete__(self, obj):
		name = self.name
		objdict = obj.__dict__
		value = None
		try:
			del objdict[name]
			return
		except KeyError as e:
			pass
		raise AttributeError(name)

	def deleter(self, delfunction):
		return getsetdelinitializer(self.getfunction, self.setfunction, delfunction)

class getsetdelinitializer(getsetinitializer):
	def __init__(self, getfunction, setfunction, delfunction):
		super().__init__(getfunction, setfunction)
		self.delfunction = delfunction

	def __delete__(self, obj):
		name = self.name
		objdict = obj.__dict__
		self.delfunction(obj)
		try:
			del objdict[name]
			return
		except KeyError:
			pass
		raise AttributeError(name)

def initializer(getfunction, setfunction=None, delfunction=None):
	if setfunction:
		if delfunction:
			return getsetdelinitializer(setfunction, getfunction, delfunction)
		return getsetinitializer(setfunction, getfunction)
	if delfunction:
		return getdelinitializer(getfunction, delfunction)
	return getinitializer(getfunction)

def xyzzy(*args, **kwargs):
	"""Nothing happens."""

class flexiblemethod:
	def __init__(self, method):
		self._method = method
		self._classmethod = method

	def classmethod(self, method):
		self._classmethod = method
		return self

	def __get__(self, instance, owner):
		if instance is None:
			return method_closure(self._classmethod, owner)
		else:
			return method_closure(self._method, instance)

class fallback:
	def __init__(self, func):
		self.func = func

	def __get__(self, instance, owner):
		if instance is None:
			instance = owner
		return self.func(instance)

def stub(f):
	def toe():
		raise NotImplementedError(f.__qualname__)
	return toe

class Clarity:
	def __init__(self, **kwargs):
		for pair in kwargs.items():
			setattr(self, *pair)

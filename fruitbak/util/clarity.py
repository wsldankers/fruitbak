class getinitializer:
	def __init__(self, getfunction):
		self.getfunction = getfunction
		self.__doc__ = getfunction.__doc__

	def __get__(self, obj, objtype = None):
		getfunction = self.getfunction
		value = getfunction(obj)
		setattr(obj, getfunction.__name__, value)
		return value

	def setter(self, setfunction):
		return getsetinitializer(self.getfunction, setfunction)

	def deleter(self, delfunction):
		return getdelinitializer(self.getfunction, delfunction)

class getdelinitializer(getinitializer):
	def __init__(self, getfunction, delfunction):
		super().__init__(getfunction)
		self.delfunction = delfunction
		self.name = getfunction.__name__

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

class getsetinitializer:
	def __init__(self, getfunction, setfunction):
		self.getfunction = getfunction
		self.setfunction = setfunction
		self.name = getfunction.__name__
		self.__doc__ = getfunction.__doc__

	def __get__(self, obj, objtype = None):
		getfunction = self.getfunction
		name = getfunction.__name__
		objdict = obj.__dict__
		try:
			return objdict[name]
		except KeyError:
			pass
		value = getfunction(obj)
		objdict[name] = value
		return value

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
			instance = owner
			method = self._classmethod
		else:
			method = self._method
		def closure(*args, **kwargs):
			return method(instance, *args, **kwargs)
		closure.__name__ = method.__name__
		closure.__qualname__ = method.__qualname__
		closure.__doc__ = method.__doc__
		return closure

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

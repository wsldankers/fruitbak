"""Various functions and classes that can be used as decorators, parent
classes and other tidbits that proved useful when writing the classes
that make up Fruitbak."""

from types import MethodType as method_closure

class _getinitializer:
	"""A non-data descriptor that runs a function as an initializer whenever
	the attribute is accessed and there is no value in the object's `__dict__`
	for it.

	The return value of the initializer is stored in the dict and will be
	returned for future reads of the attribute (unless it is overwritten or
	deleted)."""

	def __init__(self, getfunction):
		self.getfunction = getfunction
		self.__doc__ = getfunction.__doc__

	def __get__(self, obj, objtype = None):
		getfunction = self.getfunction
		name = getfunction.__name__
		try:
			objdict = vars(obj)
		except AttributeError:
			if obj is None:
				# This typically happens when querying for docstrings,
				# so return something with the appropriate docstring.
				return self
			raise

		# Do an explicit check to accommodate the threaded scenario:
		# even if this getter is protected by a lock, that lock is
		# not taken during the moment that python checks whether the
		# attribute exists in the __dict__.
		# There is an interval between python's check for the entry's
		# presence in the __dict__ and our acquiring the lock in which
		# another thread might have populated the __dict__ entry.
		try:
			return objdict[name]
		except KeyError:
			pass
		value = getfunction(obj)
		objdict[name] = value
		return value

	def setter(self, setfunction):
		"""Typically used as a decorator, this registers a function that will be
		called every time the attribute is written to. The return value of the
		supplied function is stored in the object's `__dict__` and will be returned
		for future reads of the attribute (unless it is overwritten or deleted).

		:param function setfunction: The callback for set operations
		:return: A new data descriptor
		:rtype: fruitbak.util.oo._getsetinitializer"""

		return _getsetinitializer(self.getfunction, setfunction)

	def deleter(self, delfunction):
		"""Typically used as a decorator, this registers a function that will be
		called every time the attribute is deleted.

		:param function delfunction: The callback for delete operations
		:return: A new data descriptor
		:rtype: fruitbak.util.oo._getdelinitializer"""

		return _getdelinitializer(self.getfunction, delfunction)

class _getdelinitializer(_getinitializer):
	"""A data descriptor that runs a function as an initializer whenever
	the attribute is accessed and there is no value in the object's `__dict__`
	for it. Another function is run whenever the attribute is deleted.

	The return value of the initializer is stored in the dict and will be
	returned for future reads of the attribute (unless it is overwritten or
	deleted)."""

	def __init__(self, getfunction, delfunction):
		super().__init__(getfunction)
		self.name = getfunction.__name__
		self.delfunction = delfunction

	def __set__(self, obj, value):
		vars(obj)[self.name] = value

	def __delete__(self, obj):
		name = self.name
		objdict = vars(obj)
		self.delfunction(obj)
		try:
			del objdict[name]
		except KeyError:
			pass
		else:
			return
		raise AttributeError(name)

	def setter(self, setfunction):
		"""Typically used as a decorator, this registers a function that will be
		called every time the attribute is written to. The return value of the
		supplied function is stored in the object's `__dict__` and will be returned
		for future reads of the attribute (unless it is overwritten or deleted).

		:param function setfunction: The callback for set operations
		:return: A new data descriptor
		:rtype: fruitbak.util.oo._getsetdelinitializer"""

		return _getsetdelinitializer(self.getfunction, setfunction, self.delfunction)

class _getsetinitializer(_getinitializer):
	"""A data descriptor that runs a function as an initializer whenever
	the attribute is accessed and there is no value in the object's `__dict__`
	for it. Another function is run whenever the attribute is assigned to.

	The return value of either function is stored in the object's `__dict__` and
	will be returned for future reads of the attribute (unless it is
	overwritten or deleted)."""

	def __init__(self, getfunction, setfunction):
		super().__init__(getfunction)
		self.name = getfunction.__name__
		self.setfunction = setfunction

	def __set__(self, obj, value):
		vars(obj)[self.name] = self.setfunction(obj, value)

	def __delete__(self, obj):
		name = self.name
		objdict = vars(obj)
		value = None
		try:
			del objdict[name]
		except KeyError as e:
			pass
		else:
			return
		raise AttributeError(name)

	def deleter(self, delfunction):
		"""Typically used as a decorator, this registers a function that will be
		called every time the attribute is deleted.

		:param function delfunction: The callback for delete operations
		:return: A new data descriptor
		:rtype: fruitbak.util.oo._getsetdelinitializer"""

		return _getsetdelinitializer(self.getfunction, self.setfunction, delfunction)

class _getsetdelinitializer(_getsetinitializer):
	"""A data descriptor that runs a function as an initializer whenever
	the attribute is accessed and there is no value in the object's `__dict__`
	for it, another function whenever the attribute is assigned to and a third
	whenever the attribute is deleted.

	The return value of the first two functions is stored in the object's
	`__dict__` and will be returned for future reads of the attribute (unless it
	is overwritten or deleted)."""

	def __init__(self, getfunction, setfunction, delfunction):
		super().__init__(getfunction, setfunction)
		self.delfunction = delfunction

	def __delete__(self, obj):
		name = self.name
		objdict = vars(obj)
		self.delfunction(obj)
		try:
			del objdict[name]
		except KeyError:
			pass
		else:
			return
		raise AttributeError(name)

def initializer(getfunction, setfunction=None, delfunction=None):
	"""Decorate a method to make it an initializer for an attribute. The
	function will be called whenever the attribute is accessed and there is no
	value in the object's `__dict__` for it yet.

	The return value is stored in the object's `__dict__` and will be returned
	for future reads of the attribute (unless it is overwritten or deleted).

	You can optionally supply functions that will be called whenever the
	attribute is assigned to or deleted, respectively. The return value of the
	former is stored in the object's `__dict__` just like the initializer.

	The delete function is called even if the attribute hasn't been initialized
	or otherwise set. An AttributeError will be raised later on, in that case.

	It is not necessary to supply the optional functions right away, the
	descriptor provides helper functions that can be used as decorators::

		class Fruitbasket:
			@initializer
			def bananas(self):
				"docstring goes here"
				# return an empty list for our bananas
				return []

			@bananas.setter
			def bananas(self, value):
				# ensure that self.bananas is a proper list
				return list(value)

			@bananas.deleter
			def bananas(self):
				# it would be a shame if they go to waste
				self.eat(self.bananas)

		basket = Fruitbasket()

		# bananas is auto-instantiated:
		basket.bananas.append(Banana())

		# tuple automatically converted to a list:
		basket.bananas = ()

		# so this just works now:
		basket.bananas.append(Banana())

	:param function getfunction: The function to call whenever the attribute is
		accessed but hasn't been assigned to.
	:param function setfunction: The optional function to process whatever value is
		assigned to the attribute.
	:param function delfunction: The optional function to call when the attribute
		is deleted.
	:return: A descriptor with the described functionality.
	:rtype: fruitbak.util.oo._getinitializer"""

	if setfunction is None:
		if delfunction is None:
			return _getinitializer(getfunction)

		return _getdelinitializer(getfunction, delfunction)

	if delfunction is None:
		return _getsetinitializer(getfunction, setfunction)

	return _getsetdelinitializer(getfunction, setfunction, delfunction)

def xyzzy(*args, **kwargs):
	"""Nothing happens.

	Accept any arguments and do nothing.

	:param tuple args: Ignored.
	:param dict kwargs: Ignored.
	:return: None"""

class flexiblemethod:
	"""Decorator class that enables a method to be used both as an instance method
	and as a class method. The `self` argument of your method will be an object
	of the class or the class itself, respectively.

	It is also possible to supply a separate method that will be called when the
	method is invoked on a class.

	:param function method: Called when the method is invoked on an object.
	:param function classmethod: Called when the method is invoked on the class
		(optional).

	If the `classmethod` parameter is not supplied or is `None`, the value for the
	`method` parameter is used for both instance and class invocations.

	You can also set the class method later on using the `classmethod` attribute
	as a property::

		class Introduction:
			@flexiblemethod
			def introduce(self):
				print("Hi! I'm an instance method!")

			@introduce.classmethod
			def introduce(self):
				"docstring goes here"
				print("And I'm a class method!")

		intro = Introduction()
		# introduces itself as an instance method:
		intro.introduce()

		# introduces itself as a class method:
		Introduction.introduce()

	"""

	def __init__(self, method, classmethod = None):
		# don't bother propagating __doc__, method_closure will take care of that.
		self._method = method
		if classmethod is None:
			self._classmethod = method
		else:
			self._classmethod = classmethod

	def classmethod(self, method):
		"""Set the method to use when invoked on the class. Can be used as a
		decorator.

		:param function classmethod: Called when the method is invoked on the
			class."""

		self._classmethod = method
		return self

	def __get__(self, instance, owner):
		if instance is None:
			return method_closure(self._classmethod, owner)
		else:
			return method_closure(self._method, instance)

class fallback:
	"""Decorator that creates a non-data descriptor that simply calls the
	supplied method whenever the attribute is retrieved but does not have a
	value yet.

	:param function method: The method to call when the attribute is
		retrieved.

	The return value of the function is returned to the caller as the value for
	the attribute.

	::

		class Agency:
			@fallback
			def booking(self):
				raise RuntimeError("no")

		travel = Agency()
		print(travel.booking) # raises a RuntimeError
		travel.booking = 'trip'
		print(travel.booking) # prints 'trip'
	"""

	def __init__(self, method):
		self._method = method

	def __get__(self, instance, owner):
		return self._method(owner if instance is None else instance)

def stub(method):
	"""Decorator to mark a method as a stub. Attempts to call the method
	will result in a NotImplementedError being raised. The function body
	of the original method is ignored.

	:param function method: The method that should be a stub.
	:return: A stub method."""

	qualname = method.__qualname__
	def toe():
		raise NotImplementedError(qualname)
	toe.__name__ = method.__name__
	toe.__qualname__ = qualname
	toe.__doc__ = method.__doc__
	return toe

class Initializer:
	"""Generic parent class that provides an `__init__` that simply calls
	`setattr` on all its keyword arguments. This allows for easy initialization
	of objects::

		class Example(Initializer):
			pass

		eg = Example(foo = 3, bar = 5)

		print(eg.foo) # prints 3

	:param dict kwargs: Attributes (and their values) to set.
	"""

	def __init__(self, **kwargs):
		for pair in kwargs.items():
			setattr(self, *pair)

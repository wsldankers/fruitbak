"""Various decorators that automate locking behavior for classes, methods
and descriptors. These decorators assume that the lock can be accessed
through the obj.lock attribute.

It also provides a lock type (NLock) that can be used to find cases of
undesired nested locking, that is, attempts to acquire a lock twice by the
same thread."""

from threading import Lock, RLock, Condition
from functools import wraps

class unlockedmethod:
	"""Decorator that indicates that this method should not perform automatic
	locking. Can only be used in classes that have the @lockingclass
	attribute."""

	__slots__ = 'function',

	def __init__(self, function):
		self.function = function

	def __getattr__(self, name):
		return getattr(self.function, name)

def lockedmethod(method):
	"""Decorator that acquires the lock when the method is called and
	releases it when the method is done."""

	@wraps(method)
	def replacement(self, *args, **kwargs):
		with self.lock:
			return method(self, *args, **kwargs)

	return replacement

class lockednondatadescriptor:
	"""Decorator that acquires the lock when the non-data descriptor is invoked
	and releases it when the descriptor is done.

	Please note that the lock for the __get__ handler is acquired *after*
	Python tests whether the descriptor's name is present in the object's
	`__dict__`. This means that a correct implementation of the __get__ handler
	has to manually re-test the presence of the descriptor's name in the
	`__dict__`.

	Example::

		@lockednondatadescriptor
		def foo(self):
			obj_dict = vars(self)
			try:
				return obj_dict['foo']
			except KeyError:
				pass
			value = somefunction()
			obj_dict['foo'] = value
			return value

	"""

	def __init__(self, descriptor):
		self.descriptor = descriptor

	def __get__(self, obj, *args, **kwargs):
		try:
			lock = obj.lock
		except AttributeError:
			if obj is not None:
				raise
		else:
			with lock:
				return self.descriptor.__get__(obj, *args, **kwargs)

		# This means the descriptor was invoked in the class context.
		# We have nothing we need to lock in that case, so just call
		# the original descriptor's __get__ method without a lock.
		return self.descriptor.__get__(obj, *args, **kwargs)

class lockeddatadescriptor(lockednondatadescriptor):
	"""Decorator that acquires the lock when the data descriptor is invoked
	and releases it when the descriptor is done."""

	def __set__(self, obj, *args, **kwargs):
		with obj.lock:
			return self.descriptor.__set__(obj, *args, **kwargs)

	def __delete__(self, obj, *args, **kwargs):
		with obj.lock:
			return self.descriptor.__delete__(obj, *args, **kwargs)

def lockeddescriptor(descriptor):
	"""Decorator that acquires the lock when the descriptor is invoked
	and releases it when the descriptor is done.

	Please note that the lock for the __get__ handler of non-data descriptors
	is acquired *after* Python tests whether the descriptor's name is present
	in the object's `__dict__`. This means that a correct implementation of the
	__get__ handler has to manually re-test the presence of the descriptor's
	name in the `__dict__`.

	See lockednondatadescriptor() for an example."""

	if hasattr(descriptor, '__set__'):
		return lockeddatadescriptor(descriptor)
	else:
		return lockednondatadescriptor(descriptor)

class unlockeddescriptor:
	"""Decorator that indicates that this descriptor should not perform
	automatic locking. Can only be used in classes that have the @lockingclass
	attribute."""

	__slots__ = 'descriptor',

	def __init__(self, descriptor):
		self.descriptor = descriptor

	def __getattr__(self, name):
		return getattr(self.descriptor, name)

def locked(value):
	"""Decorator that acquires the lock when the method or descriptor is
	invoked and releases the lock when it is done."""

	if isinstance(value, type(locked)):
		return lockedmethod(value)
	elif hasattr(value, '__set__'):
		return lockeddatadescriptor(value)
	elif hasattr(value, '__get__'):
		return lockednondatadescriptor(value)
	else:
		raise RuntimeError("don't know how to lock a %s" % repr(value))

def unlocked(value):
	"""Decorator that indicates that this method or descriptor should not
	perform automatic locking. Can only be used in classes that have the
	@lockingclass attribute."""

	if isinstance(value, type(unlocked)):
		return unlockedmethod(value)
	elif hasattr(value, '__get__'):
		return unlockeddescriptor(value)
	else:
		return value

def lockingclass(cls):
	"""Decorator for classes that automatically makes all methods and
	descriptor acquire the lock. It also automatically initializes
	self.lock to an instance of threading.RLock.

	Any methods or descriptors that should not perform automatic locking
	can be decorated with @unlocked."""

	replacements = {}
	for key, value in vars(cls).items():
		if key == '__init__':
			oldinit = value
			@wraps(oldinit)
			def __init__(self, *args, **kwargs):
				self.lock = RLock()
				return oldinit(self, *args, **kwargs)
			replacements[key] = __init__
		elif key == '__dict__':
			pass
		elif isinstance(value, unlockedmethod):
			replacements[key] = value.function
		elif isinstance(value, unlockeddescriptor):
			replacements[key] = value.descriptor
		elif callable(value):
			replacements[key] = lockedmethod(value)
		elif hasattr(value, '__set__'):
			replacements[key] = lockeddatadescriptor(value)
		elif hasattr(value, '__get__'):
			replacements[key] = lockednondatadescriptor(value)
		else:
			replacements[key] = value

	if '__init__' not in replacements:
		def __init__(self, *args, **kwargs):
			self.lock = RLock()
			return super(cls, self).__init__(*args, **kwargs)
		replacements['__init__'] = __init__
	
	# make sure the cls in super() above works properly
	cls = type(cls.__name__, cls.__bases__, replacements)
	return cls

if __debug__:
	class NLock(type(RLock())):
		"""An alias for threading.Lock (if __debug__ is False) or
		threading.RLock (if __debug__ is True). The latter can be queried by
		casting it to a bool (or simply by using it as a truth test). It will
		evaluate to True iff it is locked by the current thread.

		Because this boolean test is only available if __debug__ is True, you
		should only use it in assert statements (which are elided when __debug__ is
		False).

		If __debug__ is True, the NLock will, when it is acquired, detect the
		situation where it is acquired twice and throw a RuntimeError if that is
		the case."""

		def acquire(self, *args, **kwargs):
			if self._is_owned():
				raise RuntimeError("lock already held by same thread")
			return super().acquire(*args, **kwargs)

		def __enter__(self):
			if self._is_owned():
				raise RuntimeError("lock already held by same thread")
			return super().__enter__()

		def __bool__(self):
			return self._is_owned()

	class NCondition(Condition):
		"""A subclass of `threading.Condition` that uses an `NLock`. It supports
		boolean testing to see if the lock is held by the current thread."""

		def __init__(self, lock = None):
			if lock is None:
				lock = NLock()
			self.__bool__ = lock.__bool__
			return super().__init__(lock)
else:
	NLock = Lock
	NCondition = Condition

"""Various decorators that automate locking behavior for classes, methods
and descriptors. These decorators assume that the lock can be accessed
through the obj.lock attribute.

It also provides a lock type (NLock) that can be used to find cases of
undesired nested locking, that is, attempts to acquire a lock twice by the
same thread."""

from threading import Lock, RLock
from functools import wraps

class unlockedmethod:
	"""Decorator that indicates that this method should not perform automatic
	locking. Can only be used in classes that have the @lockingclass
	attribute."""

	def __init__(self, function):
		self.function = function

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
	__dict__. This means that a correct implementation of the __get__ handler
	has to manually re-test the presence of the descriptor's name in the
	__dict__.

	Example::

		@lockednondatadescriptor
		def foo(self):
			obj_dict = self.__dict__
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
		with obj.lock:
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
	in the object's __dict__. This means that a correct implementation of the
	__get__ handler has to manually re-test the presence of the descriptor's
	name in the __dict__.

	See lockednondatadescriptor() for an example."""

	if hasattr(descriptor, '__set__'):
		return lockeddatadescriptor(descriptor)
	else:
		return lockednondatadescriptor(descriptor)

class unlockeddescriptor:
	"""Decorator that indicates that this descriptor should not perform
	automatic locking. Can only be used in classes that have the @lockingclass
	attribute."""

	def __init__(self, descriptor):
		self.descriptor = descriptor

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
	for key, value in cls.__dict__.items():
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
		def __enter__(self):
			ctx = super().__enter__()
			try:
				if " count=1 " not in repr(self):
					raise RuntimeError("lock already held by same thread")
			except:
				self.__exit__(None, None, None)
				raise
			return ctx

		def acquire(self, *args, **kwargs):
			r = super().acquire(*args, **kwargs)
			if r:
				try:
					if " count=1 " not in repr(self):
						raise RuntimeError("lock already held by same thread")
				except:
					self.release()
					raise
			return r

		def __bool__(self):
			s = super()
			if not s.acquire(False):
				return False
			try:
				return " count=1 " not in repr(self)
			finally:
				s.release()
else:
	class NLock(type(Lock())):
		def __bool__(self):
			s = super()
			if s.acquire(False):
				s.release()
				return True
			else:
				return False

NLock.__doc__ = """A subclass of either threading.Lock (if __debug__ is
False) or threading.RLock (if __debug__ is True) that can be queried by
casting it to a bool (or simply by using it as a truth test). It will
evaluate to True iff it is locked. This may sometimes yield false positives
due to inherent race conditions, so only use this for sanity checks such as
`assert`.

If __debug__ is True, the NLock will, when it is acquired, detect the
situation where it is acquired twice and throw a RuntimeError if that is
the case."""

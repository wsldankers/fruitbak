#! /usr/bin/python3

import threading

class unlockedmethod:
	def __init__(self, function):
		self.function = function

def lockedmethod(method):
	def replacement(self, *args, **kwargs):
		with self.lock:
			return method(self, *args, **kwargs)
	replacement.__name__ = method.__name__
	return replacement

class lockednondatadescriptor:
	def __init__(self, descriptor):
		self.descriptor = descriptor

	def __get__(self, obj, *args, **kwargs):
		with obj.lock:
			return self.descriptor.__get__(obj, *args, **kwargs)

class lockeddatadescriptor(lockednondatadescriptor):
	def __set__(self, obj, *args, **kwargs):
		with obj.lock:
			return self.descriptor.__set__(obj, *args, **kwargs)

	def __delete__(self, obj, *args, **kwargs):
		with obj.lock:
			return self.descriptor.__delete__(obj, *args, **kwargs)

def lockeddescriptor(descriptor):
	if hasattr(descriptor, '__set__'):
		return lockeddatadescriptor(descriptor)
	else:
		return lockednondatadescriptor(descriptor)

class unlockeddescriptor:
	def __init__(self, descriptor):
		self.descriptor = descriptor

def locked(value):
	if isinstance(value, type(locked)):
		return lockedmethod(value)
	elif hasattr(value, '__set__'):
		return lockeddatadescriptor(value)
	elif hasattr(value, '__get__'):
		return lockednondatadescriptor(value)
	else:
		raise RuntimeError("don't know how to lock a %s" % repr(value))

def unlocked(value):
	if isinstance(value, type(unlocked)):
		return unlockedmethod(value)
	elif hasattr(value, '__get__'):
		return unlockeddescriptor(value)
	else:
		return value

def lockingclass(cls):
	replacements = {}
	function = type(lockingclass)
	for key, value in cls.__dict__.items():
		if key == '__init__':
			oldinit = value
			def __init__(self, *args, **kwargs):
				self.lock = threading.RLock()
				return oldinit(self, *args, **kwargs)
			replacements[key] = __init__
		elif key == '__dict__':
			pass
		elif isinstance(value, unlockedmethod):
			replacements[key] = value.function
		elif isinstance(value, unlockeddescriptor):
			replacements[key] = value.descriptor
		elif isinstance(value, function):
			replacements[key] = lockedmethod(value)
		elif hasattr(value, '__set__'):
			replacements[key] = lockeddatadescriptor(value)
		elif hasattr(value, '__get__'):
			replacements[key] = lockednondatadescriptor(value)
		else:
			replacements[key] = value
	return type(cls.__name__, cls.__bases__, replacements)

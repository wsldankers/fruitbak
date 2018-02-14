#! /usr/bin/python3

import weakref
from threading import RLock

class weakproperty(property):
	def __init__(prop, f):
		name = f.__name__

		def getter(self):
			dict = self.__dict__
			try:
				weak = dict[name]
			except KeyError:
				value = f(self)
				def unsetter(weak):
					try:
						if dict[name] is weak:
							del dict[name]
					except KeyError:
						pass
				weak = weakref.ref(value, unsetter)
				dict[name] = weak
				return value
			return weak()

		def setter(self, value):
			dict = self.__dict__
			def unsetter(weak):
				try:
					if dict[name] is weak:
						del dict[name]
				except KeyError:
					pass
			dict[name] = weakref.ref(value, unsetter)

		def deleter(self):
			del self.__dict__[name]

		super().__init__(getter, setter, deleter)

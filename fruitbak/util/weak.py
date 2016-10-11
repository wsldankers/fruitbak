#! /usr/bin/python3

import weakref

class weakproperty(property):
	def __init__(self, f):
		dict = self.__dict__
		name = f.__name__
		def getter():
			return dict[name]()

		def deleter():
			del dict[name]

		def setter(self, value):
			dict[name] = weakref.ref(value, deleter)

		super().__init__(getter, setter, deleter)

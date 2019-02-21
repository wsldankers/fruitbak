"""Collection of weakref-related utility functions

This "collection" of weakref-related utility functions currently only
contains one class (weakproperty)."""

import weakref

class weakproperty(property):
	"""A property that keeps a weak reference.

	Unlike a normal property it doesn't call functions for
	getting/setting/deleting but just stores the value in the object's
	dictionary. The function you'd decorate with this property is used
	as an initializer that is called when the attribute is retrieved and
	the property was either never set, it was deleted, or the weak
	reference was lost."""

	def __init__(prop, f):
		"""Create a new weakproperty using f as the intializer function.

		The name of the supplied initializer function is also as the key
		into the object's dictionary."""
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

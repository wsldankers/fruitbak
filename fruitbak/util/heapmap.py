"""Heap queue object that also allows dictionary style access.

A variation on the heap queue that allows you to add and remove entries
as if it was a dictionary. The value of each item you add is used for
comparison (using the < operator) when maintaining the heap property.

It comes in two variants, one (MinHeapMap) that extracts the smallest
element when you call pop(), and one (MaxHeapMap) that extracts the
largest.

Inconsistent results from the comparison functions will result in an
inconsistent heap.

This implementation keeps the heap consistent even if the comparison
functions of the items throw an exception. It is threadsafe."""

# TODO: implement move_to_end(key, True) using the counter
# TODO: use collections.abc.MutableMapping as base class
# TODO: use collections.abc.MappingView as base class

from .oo import stub
from .locking import lockingclass, unlocked, locked
from collections.abc import Set
from collections import namedtuple

# Internal class that represents a node in the heapmap.
class HeapMapNode:
	__slots__ = ('key', 'counter', 'value', 'index')
	def __init__(self, key, counter, value, index):
		self.key = key
		self.counter = counter
		self.value = value
		self.index = index

	def __lt__(self, other):
		return self.counter < other.counter if self.value == other.value else self.value < other.value

	def __repr__(self):
		return 'HeapMapNode(key = %r, counter = %d, value = %r, index = %d)' % (self.key, self.counter, self.value, self.index)

# Internal class that is returned for HeapMap.values()
class HeapMapValueView:
	__slots__ = ('mapping')

	def __init__(self, heapmap):
		self.mapping = heapmap.mapping

	def __len__(self):
		return len(self.mapping)

	def __iter__(self):
		for node in self.mapping:
			yield node.value

	def __contains__(self, value):
		for node in self.mapping:
			if node.value == value:
				return True
		return False

# Internal class that is returned for HeapMap.items()
class HeapMapItemView(Set):
	__slots__ = ('mapping')

	def __init__(self, heapmap):
		self.mapping = heapmap.mapping

	def __len__(self):
		return len(self.mapping)

	def __iter__(self):
		for node in heapmap.mapping:
			yield node.key, node.value

	def __contains__(self, item):
		key, value = item
		try:
			node = self.mapping[key]
		except KeyError:
			return False
		return node.value == value

# datatype: supports both extractmax and fetching by key
@lockingclass
class HeapMap:
	"""__init__(items = None, **kwargs)

	Base class for MinHeapMap and MaxHeapMap. Do not instantiate
	directly, use one of the subclasses.

	Initialized using the same interface as dict().

	:param items: Add this dict or an iterable of size 2 iterables
		to the HeapMap as initial values.
	:type items: dict or iter(iter)
	:param dict kwargs: Add all keyword items to the HeapMap as
		initial values.
	"""

	_counter = 0

	def __init__(self, items = None, **kwargs):
		heap = []
		mapping = {}
		counter = self._counter
		counter_increment = self._counter_increment

		if items is None:
			pass
		elif hasattr(items, 'items'):
			for key, value in items.items():
				mapping[key] = container = HeapMapNode(key, counter, value, len(heap))
				counter += counter_increment
				heap.append(container)
		elif hasattr(items, 'keys'):
			for key in items.keys():
				mapping[key] = container = HeapMapNode(key, counter, items[key], len(heap))
				counter += counter_increment
				heap.append(container)
		else:
			for key, value in items:
				mapping[key] = container = HeapMapNode(key, counter, value, len(heap))
				counter += counter_increment
				heap.append(container)
		for key, value in kwargs.items():
			mapping[key] = container = HeapMapNode(key, counter, value, len(heap))
			counter += counter_increment
			heap.append(container)

		# the following loop runs in amortized O(n) time:
		heap_len = len(heap)
		for i in range((heap_len - 1) // 2, -1, -1):
			index = i
			container = heap[index]
			value = container.value
			while True:
				child_index = index * 2 + 1
				if child_index >= heap_len:
					break
				child = heap[child_index]
				other_child_index = child_index + 1
				if other_child_index < heap_len:
					other_child = heap[other_child_index]
					if self._compare(other_child, child):
						child = other_child
						child_index = other_child_index
				if self._compare(child, container):
					heap[index] = child
					child.index = index
					index = child_index
				else:
					break
			if index != i:
				heap[index] = container
				container.index = index
				
		self.heap = heap
		self.mapping = mapping
		self._counter = counter

	def __str__(self):
		ret = ""
		for v in self.heap:
			ret += str(v.key) + " " + str(v.value) + " " + str(v.index) + "\n"
		for (k, v) in self.mapping.items():
			ret += str(v.key) + " " + str(v.value) + " " + str(v.index) + " (" + str(k) + ")\n"
		return ret

	@unlocked
	def __bool__(self):
		return bool(self.heap)

	@unlocked
	def __iter__(self):
		return iter(self.mapping)

	@unlocked
	def __contains__(self, key):
		return key in self.mapping

	@unlocked
	def __len__(self):
		return len(self.heap)

	@unlocked
	def __getitem__(self, key):
		return self.mapping[key].value

	def __setitem__(self, key, value):
		mapping = self.mapping
		heap = self.heap
		heap_len = len(heap)
		answers = []
		container = mapping.get(key)
		if container is None:
			index = heap_len

			index = heap_len
			counter = self._counter
			container = HeapMapNode(key, counter, value, index)
			self._counter = counter + 1

			while index:
				parent_index = (index - 1) // 2
				parent = heap[parent_index]
				is_greater = bool(self._compare(container, parent))
				answers.append(is_greater)
				if is_greater:
					index = parent_index
				else:
					break

			mapping[key] = container
			heap.append(container)
			answers.reverse()

			while index:
				parent_index = (index - 1) // 2
				parent = heap[parent_index]
				if answers.pop():
					heap[index] = parent
					parent.index = index
					index = parent_index
				else:
					break

			container.index = index
			heap[index] = container
		else:
			index = container.index
			comparison = HeapMapNode(container.key, container.counter, value, container.index)

			while index:
				parent_index = (index - 1) // 2
				parent = heap[parent_index]
				is_greater = bool(self._compare(comparison, parent))
				answers.append(is_greater)
				if is_greater:
					index = parent_index
				else:
					break

			if index == container.index:
				while True:
					child_index = index * 2 + 1
					if child_index >= heap_len:
						break
					child = heap[child_index]
					other_child_index = child_index + 1
					if other_child_index < heap_len:
						other_child = heap[other_child_index]
						is_greater = bool(self._compare(other_child, child))
						answers.append(is_greater)
						if is_greater:
							child = other_child
							child_index = other_child_index
					is_greater = bool(self._compare(child, comparison))
					answers.append(is_greater)
					if is_greater:
						index = child_index
					else:
						break

			index = container.index
			answers.reverse()

			while index:
				parent_index = (index - 1) // 2
				parent = heap[parent_index]
				if answers.pop():
					heap[index] = parent
					parent.index = index
					index = parent_index
				else:
					break

			if index == container.index:
				while True:
					child_index = index * 2 + 1
					if child_index >= heap_len:
						break
					child = heap[child_index]
					other_child_index = child_index + 1
					if other_child_index < heap_len:
						other_child = heap[other_child_index]
						if answers.pop():
							child = other_child
							child_index = other_child_index
					if answers.pop():
						heap[index] = child
						child.index = index
						index = child_index
					else:
						break

			container.value = value
			if container.index != index:
				container.index = index
				heap[index] = container

	def __delitem__(self, key):
		mapping = self.mapping
		victim = mapping[key]
		index = victim.index

		heap = self.heap
		heap_len = len(heap) - 1

		if index == heap_len:
			heap.pop()
			del mapping[key]
			return

		replacement = heap[heap_len]
		value = replacement.value
		answers = []

		# don't try to bubble up if the deleted item was the
		# parent of the popped item
		if index != (heap_len - 1) // 2:
			while index:
				parent_index = (index - 1) // 2
				parent = heap[parent_index]
				is_greater = bool(self._compare(replacement, parent))
				answers.append(is_greater)
				if is_greater:
					index = parent_index
				else:
					break

		if index == victim.index:
			while True:
				child_index = index * 2 + 1
				if child_index >= heap_len:
					break
				child = heap[child_index]
				other_child_index = child_index + 1
				if other_child_index < heap_len:
					other_child = heap[other_child_index]
					is_greater = bool(self._compare(other_child, child))
					answers.append(is_greater)
					if is_greater:
						child = other_child
						child_index = other_child_index
				is_greater = bool(self._compare(child, replacement))
				answers.append(is_greater)
				if is_greater:
					index = child_index
				else:
					break

		del mapping[key]
		heap.pop()
		index = victim.index
		answers.reverse()

		# don't try to bubble up if the deleted item was the
		# parent of the popped item
		if index != (heap_len - 1) // 2:
			while index:
				parent_index = (index - 1) // 2
				parent = heap[parent_index]
				if answers.pop():
					heap[index] = parent
					parent.index = index
					index = parent_index
				else:
					break

		if index == victim.index:
			while True:
				child_index = index * 2 + 1
				if child_index >= heap_len:
					break
				child = heap[child_index]
				other_child_index = child_index + 1
				if other_child_index < heap_len:
					other_child = heap[other_child_index]
					if answers.pop():
						child = other_child
						child_index = other_child_index
				if answers.pop():
					heap[index] = child
					child.index = index
					index = child_index
				else:
					break

		heap[index] = replacement
		replacement.index = index

	def pop(self, key = None):
		"""Remove and return the value in the heap corresponding to the specified key.
		If key is absent or None, remove and return the smallest/largest value in the heap.

		:param key: The key of the value to remove and return.
		:return: The value corresponding to key."""

		if key is None:
			ret = self.heap[0]
			del self[ret.key]
		else:
			ret = self[key]
			del self[key]
		return ret.value

	def popkey(self, key = None):
		"""Remove and return the key in the heap equal to the specified key.
		If key is absent or None, remove and return the smallest/largest value in the heap.

		:param key: The key of the value to remove and return.
		:return: The value corresponding to key."""

		if key is None:
			ret = self.heap[0]
			del self[ret.key]
		else:
			ret = self[key]
			del self[key]
		return ret.key

	def popitem(self, key = None):
		"""Remove and return a tuple of the key and the value in the heap
		corresponding to the specified key. If key is absent or None, remove and
		return the smallest/largest item in the heap.

		:param key: The key of the item to remove and return.
		:return: A tuple of (key, value) corresponding to key."""

		if key is None:
			ret = self.heap[0]
			key = ret.key
		else:
			ret = self[key]
		del self[key]
		return key, ret.value

	@unlocked
	def peek(self):
		"""Return the value in the heap corresponding to the specified key.
		If key is absent or None, return the smallest value in the heap.

		:param key: The key of the value to return.
		:return: The value corresponding to key."""

		return self.heap[0].value

	@unlocked
	def peekitem(self):
		"""Return a tuple of the key and the value in the heap corresponding to the
		specified key. If key is absent or None, return the smallest item in the
		heap.

		:param key: The key of the item to return.
		:return: A tuple of (key, value) corresponding to key."""

		item = self.heap[0]
		return item.key, item.value

	@unlocked
	def keys(self):
		"""Return a view of the keys.

		:return: A view of the keys of the mapping."""

		return self.mapping.keys()

	@unlocked
	def values(self):
		"""Return a view of the values.

		:return: A view of the values of the mapping."""

		return HeapMapValueView(self)

	@unlocked
	def items(self):
		"""Return a view of the keys and values.

		:return: A view of the keys and values of the mapping, in a tuple each."""

		return HeapMapItemView(self)

	def clear(self):
		"""Empty the HeapMap by removing all keys and values."""

		self.heap.clear()
		self.mapping.clear()

	def copy(self):
		"""Create a shallow copy of this HeapMap.

		:return: The new HeapMap."""

		return type(self)(self.items())

	@unlocked
	@classmethod
	def fromkeys(cls, iterable, value = None):
		"""Create a new HeapMap with keys from iterable and values set to value.

		fromkeys() is a class method that returns a new HeapMap.

		:param iter iterable: The iterable that provides the keys.
		:param value: The value to use for all items. Defaults to None.
		:return: The new HeapMap."""

		return cls(dict.fromkeys(iterable, value))

	def setdefault(self, key, default = None):
		"""If key is in the HeapMap, return its value. If not, insert key with a
		value of default and return default.

		:param key: The key to look op and/or insert.
		:param default: The value to use if the key is not in the HeapMap.
		:return: Either the existing value or the default."""

		mapping = self.mapping
		try:
			node = mapping[key]
		except KeyError:
			pass
		else:
			return node.value
		self[key] = default
		return default

	def update(self, other = None, **kwargs):
		"""Update the dictionary with the key/value pairs from other, overwriting
		existing keys. Return None.

		update() accepts either another dictionary-like object and/or an iterable
		of key/value pairs (as tuples or other iterables of length two).
		If keyword arguments are specified, the HeapMap is then updated with
		those key/value pairs: h.update(red = 1, blue = 2).

		:param other: Add these items to the HeapMap.
		:type other: dict or iter(iter)
		:param dict kwargs: Add all keyword items to the HeapMap."""

		if self:
			if items is None:
				pass
			elif hasattr(items, 'items'):
				for key, value in items.items():
					self[key] = value
			elif hasattr(items, 'keys'):
				for key in items.keys():
					self[key] = items[key]
			else:
				for key, value in items:
					self[key] = value
			for key, value in kwargs.items():
				self[key] = value
		else:
			self.__init__(items, **kwargs)

	@stub
	def _compare(self, a, b):
		pass

	@stub
	def reversed(self):
		"""Create a new HeapMap that pops in the opposite direction, i.e., returns
		the largest item instead of the smallest or vice-versa. This is a shallow copy.

		:return: The new HeapMap."""

class MinHeapMap(HeapMap):
	"""__init__(items = None, **kwargs)

	HeapMap variant that makes pop() return the smallest element.

	Initialized using the same interface as dict().

	:param items: Add this dict or an iterable of size 2 iterables
		to the HeapMap as initial values.
	:type items: dict or iter(iter)
	:param dict kwargs: Add all keyword items to the HeapMap as
		initial values."""

	_counter_increment = 1

	def _compare(self, a, b):
		return a < b

	@locked
	def reversed(self):
		return MaxHeapMap(self)

class MaxHeapMap(HeapMap):
	"""__init__(items = None, **kwargs)

	HeapMap variant that makes pop() return the largest element.

	Initialized using the same interface as dict().

	:param items: Add this dict or an iterable of size 2 iterables
		to the HeapMap as initial values.
	:type items: dict or iter(iter)
	:param dict kwargs: Add all keyword items to the HeapMap as
		initial values."""

	_counter_increment = -1

	def _compare(self, a, b):
		return b < a

	@locked
	def reversed(self):
		return MinHeapMap(self)

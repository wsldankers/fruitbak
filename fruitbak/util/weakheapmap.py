"""Heap queue object with dictionary style access using weakref keys.

A variation on the heap queue that allows you to add and remove entries as
if it was a dictionary. The value of each item you add is used for
comparison when maintaining the heap property. This comparison is done
using the < operator exclusively, so for custom value objects you only need
to implement __lt__().

An important difference with both fruitbak.util.HeapMap and
weakref.WeakKeyDictionary is that keys are identified purely by object
identity.

It comes in two variants, one (MinWeakHeapMap) that extracts the smallest
element when you call pop(), and one (MaxWeakHeapMap) that extracts the
largest.

Entries with equal values are extracted in insertion order. Iteration is in
insertion order if your Python's dict implementation iterates in insertion
order (Python >3.7).

Inconsistent results from the comparison functions will result in an
inconsistent heap. Comparison functions with side effects cause undefined
behavior if these side effects affect the WeakHeapMap.

This implementation keeps the heap consistent even if the comparison
functions of the items throw an exception. It is threadsafe."""

from .oo import stub
from .locking import lockingclass, unlocked, locked
from weakref import ref as weakref, KeyedRef
from sys import stderr
from collections.abc import MutableMapping, KeysView, ValuesView, ItemsView
from traceback import print_exc

class _NoValue:
	__slots__ = ()
_no_value = _NoValue()

# Internal class to fake a node for key lookup purposes.
# Real WeakHeapMapNodes are expensive because they inherit from weakref.
class FakeKeyWeakHeapMapNode:
	__slots__ = 'id', 'hash'

	def __init__(self, id):
		self.id = id
		self.hash = hash(id)

	def __hash__(self):
		return self.hash

	def __eq__(self, other):
		return self.id == other.id

# Internal class to fake a node for value comparison purposes.
# Real WeakHeapMapNodes are expensive because they inherit from weakref.
class FakeValueWeakHeapMapNode:
	__slots__ = 'value', 'serial'

	def __init__(self, value, serial):
		self.value = value
		self.serial = serial

# Internal class that represents a node in the weakheapmap.
class WeakHeapMapNode(weakref):
	__slots__ = 'value', 'index', 'serial', 'id', 'hash'

	def __new__(type, key, finalizer, value, index, serial):
		return super(type, WeakHeapMapNode).__new__(type, key, finalizer)

	def __init__(self, key, finalizer, value, index, serial):
		super().__init__(key, finalizer)

		key_id = id(key)

		self.value = value
		self.index = index
		self.serial = serial
		self.id = key_id
		self.hash = hash(key_id)

	def __hash__(self):
		return self.hash

	def __eq__(self, other):
		return self.id == other.id

# Internal class that is returned for HeapMap.keys()
class WeakHeapMapKeysView(KeysView):
	def __init__(self, heapmap):
		super().__init__(heapmap.mapping)

	def __contains__(self, key):
		return FakeKeyWeakHeapMapNode(id(key)) in self._mapping

	def __iter__(self):
		for node in self._mapping:
			key = node()
			if key is not None:
				yield key

# Internal class that is returned for WeakHeapMap.values()
class WeakHeapMapValuesView(ValuesView):
	def __init__(self, heapmap):
		super().__init__(heapmap.mapping)

	def __contains__(self, value):
		for node in self.mapping:
			if node.value == value:
				return True
		return False

	def __iter__(self):
		for node in self.mapping:
			yield node.value

# Internal class that is returned for HeapMap.items()
class WeakHeapMapItemsView(ItemsView):
	def __init__(self, heapmap):
		super().__init__(heapmap.mapping)

	def __contains__(self, item):
		key, value = item
		try:
			node = self.mapping[FakeKeyWeakHeapMapNode(id(key))]
		except KeyError:
			return False
		v = node.value
		return v is value or v == value

	def __iter__(self):
		for node in heapmap.mapping:
			key = node()
			if key is not None:
				yield key, node.value

# datatype: supports both extractmin and fetching by key
@lockingclass
class WeakHeapMap(MutableMapping):
	"""__init__(items = None, **kwargs)

	Base class for MinWeakHeapMap and MaxWeakHeapMap. Do not instantiate
	directly, use one of the subclasses.

	Initialized using the same interface as dict().

	:param items: Add this dict or an iterable of size 2 iterables
		to the WeakHeapMap as initial values.
	:type items: dict or iter(iter)
	:param dict kwargs: Add all keyword items to the HeapMap as
		initial values.
	"""

	_serial = 0

	def __init__(self, items = None, **kwargs):
		weakself = weakref(self)

		def finalizer(node):
			heapmap = weakself()
			if heapmap is not None:
				with heapmap.lock:
					try:
						found = heapmap.heap[node.index]
					except IndexError:
						pass
					except:
						print_exc(file = stderr)
					else:
						if found is node:
							heapmap._delnode(node)

		self.heap = []
		self.mapping = {}
		self.finalizer = finalizer

		self._fill_initial(items, kwargs)

	@unlocked
	def _fill_initial(self, items, kwargs):
		heap = self.heap
		mapping = self.mapping
		finalizer = self.finalizer

		assert not heap

		heap_len = 0

		try:
			if items is None:
				pass
			elif hasattr(items, 'items'):
				for key, value in items.items():
					container = WeakHeapMapNode(key, finalizer, value, len(heap), heap_len)
					mapping[container] = container
					heap.append(container)
					heap_len += 1
			elif hasattr(items, 'keys'):
				for key in items.keys():
					container = WeakHeapMapNode(key, finalizer, items[key], len(heap), heap_len)
					mapping[container] = container
					heap.append(container)
					heap_len += 1
			else:
				for key, value in items:
					container = WeakHeapMapNode(key, finalizer, value, len(heap), heap_len)
					mapping[container] = container
					heap.append(container)
					heap_len += 1
			for key, value in kwargs.items():
				container = WeakHeapMapNode(key, finalizer, value, len(heap), heap_len)
				mapping[container] = container
				heap.append(container)
				heap_len += 1

			# the following loop runs in amortized O(n) time:
			heap_len = len(heap)
			for i in range((heap_len - 1) // 2, -1, -1):
				index = i
				container = heap[index]
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

			self._serial = heap_len
		except:
			heap.clear()
			mapping.clear()
			raise

	def __str__(self):
		ret = ""
		for v in self.heap:
			ret += str(v.weakkey()) + " " + str(v.value) + " " + str(v.index) + "\n"
		return ret

	@unlocked
	def __bool__(self):
		return bool(self.heap)

	@unlocked
	def __iter__(self):
		mapping = self.mapping
		for node in mapping:
			key = node()
			if key is not None:
				yield key

	@unlocked
	def __contains__(self, key):
		return FakeKeyWeakHeapMapNode(id(key)) in self.mapping

	@unlocked
	def __len__(self):
		return len(self.heap)

	@unlocked
	def __getitem__(self, key):
		return self.mapping[FakeKeyWeakHeapMapNode(id(key))].value

	def get(key, default = None):
		"""Return the value for `key` if `key` is in the mapping, else `default`.
		If `default` is not given, it defaults to `None`, so that this method never
		raises a `KeyError`.

		:param key: The key to look up.
		:param default: The value to return if `key` was not found.
		:return: The value belonging to `key` or `default` if it was not found."""

		mapping = self.mapping
		fakekey = FakeKeyWeakHeapMapNode(id(key))
		try:
			node = mapping[fakekey]
		except KeyError:
			pass
		else:
			node_key = node()
			if node_key is not None:
				return node.value

		return default

	def __setitem__(self, key, value):
		self._setitem(key, value)

	def add(self, key, value):
		"""Add the `key` to the mapping with value `value`. If the key already
		exists it will be overwritten.

		:param key: The key to add.
		:param value: The value to add."""

		self._setitem(key, value)

	@unlocked
	def _setitem(self, key, value):
		self._setnode(self.mapping.get(FakeKeyWeakHeapMapNode(id(key))), key, value)

	@unlocked
	def _setnode(self, container, key, value):
		mapping = self.mapping
		heap = self.heap
		heap_len = len(heap)
		answers = []
		container_key = None if container is None else container()
		if container_key is None:
			if container is not None:
				# So it exists, but is dead. To prevent a race condition,
				# we give it a unique, different id so it doesn't conflict
				# with the new key being added. For this we use the id()
				# of the container itself since it's unique and occupied.
				del mapping[container]
				container_id = id(container)
				container.id = container_id
				container.hash = hash(container_id)
				mapping[container] = container

			index = heap_len
			serial = self._serial
			container = WeakHeapMapNode(key, self.finalizer, value, index, serial)

			while index:
				parent_index = (index - 1) // 2
				parent = heap[parent_index]
				is_greater = bool(self._compare(container, parent))
				answers.append(is_greater)
				if is_greater:
					index = parent_index
				else:
					break

			index = heap_len
			mapping[container] = container
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

			self._serial = serial + 1
			container.index = index
			heap[index] = container
		else:
			index = container.index
			comparison = FakeValueWeakHeapMapNode(value, container.serial)

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
		node = self.mapping[FakeKeyWeakHeapMapNode(id(key))]
		node_key = node()
		if node_key is None:
			raise KeyError(key)
		self._delnode(node)

	def remove(self, key):
		"""Remove the item corresponding to the specified key.
		Raises `KeyError` if `key` is not contained in the set.

		:param key: The key to remove."""

		node = self.mapping[FakeKeyWeakHeapMapNode(id(key))]
		node_key = node()
		if node_key is None:
			raise KeyError(key)
		self._delnode(node)

	def discard(self, key):
		"""Remove the item corresponding to the specified key, if it is present.

		:param key: The key to remove."""

		mapping = self.mapping
		fakekey = FakeKeyWeakHeapMapNode(id(key))
		try:
			node = mapping[fakekey]
		except KeyError:
			pass
		else:
			node_key = node()
			if node_key is not None:
				self._delnode(node)

	@unlocked
	def _delnode(self, victim):
		index = victim.index

		mapping = self.mapping
		heap = self.heap
		heap_len = len(heap) - 1

		if index == heap_len:
			heap.pop()
			del mapping[victim]
			return

		replacement = heap[heap_len]
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

		del mapping[victim]
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

	@unlocked
	def pop(self, key = None):
		"""Remove and return the value in the heap corresponding to the specified key.
		If key is absent or None, remove and return the smallest/largest value in the heap.

		:param key: The key of the value to remove and return.
		:return: The value corresponding to key."""

		return self.popitem(key)[1]

	@unlocked
	def popkey(self, key = None):
		"""Remove and return the key in the heap equal to the specified key.
		If key is absent or None, remove and return the smallest/largest value in the heap.

		:param key: The key of the value to remove and return.
		:return: The value corresponding to key."""

		return self.popitem(key)[0]

	def popitem(self, key = None):
		"""Remove and return a tuple of the key and the value in the heap
		corresponding to the specified key. If key is absent or None, remove and
		return the smallest/largest item in the heap.

		:param key: The key of the item to remove and return.
		:return: A tuple of (key, value) corresponding to key."""

		if key is None:
			ret = self.heap[0]
			while True:
				ret = self.heap[0]
				ret_key = ret()
				self._delnode(ret)
				if ret_key is not None:
					return ret_key, ret.value
		else:
			ret = self.mapping[FakeKeyWeakHeapMapNode(id(key))]
			ret_key = ret()
			if ret_key is None:
				raise KeyError(key)
			self._delnode(ret)
			return ret_key, ret.value

	@unlocked
	def peek(self, key = None):
		"""Return the value in the heap corresponding to the specified key.
		If key is absent or None, return the smallest value in the heap.

		:param key: The key of the value to return.
		:return: The value corresponding to key."""

		return self.peekitem(key)[1]

	@unlocked
	def peekkey(self, key = None):
		"""Return the key in the heap corresponding to the specified key.
		If key is absent or None, return the smallest value in the heap.

		:param key: The key to look up and return.
		:return: The value corresponding to key."""

		return self.peekitem(key)[0]

	def peekitem(self, key = None):
		"""Return a tuple of the key and the value in the heap corresponding to the
		specified key. If key is absent or None, return the smallest item in the
		heap.

		:param key: The key of the item to return.
		:return: A tuple of (key, value) corresponding to key."""

		if key is None:
			while True:
				ret = self.heap[0]
				ret_key = ret()
				if ret_key is not None:
					return ret_key, ret.value
				self._delnode(ret)
		else:
			ret = self.mapping[FakeKeyWeakHeapMapNode(id(key))]
			ret_key = ret()
			if ret_key is None:
				raise KeyError(key)
			return ret_key, ret.value

	@unlocked
	def keys(self):
		"""Return a view of the keys.

		:return: A view of the keys of the mapping."""

		return WeakHeapMapKeysView(self)

	@unlocked
	def values(self):
		"""Return a view of the values.

		:return: A view of the values of the mapping."""

		return WeakHeapMapValuesView(self)

	@unlocked
	def items(self):
		"""Return a view of the keys and values.

		:return: A view of the keys and values of the mapping, in a tuple each."""

		return WeakHeapMapItemsView(self)

	def clear(self):
		"""Empty the WeakHeapMap by removing all keys and values."""

		self.heap.clear()
		self.mapping.clear()
		self._serial = 0

	def copy(self):
		"""Create a shallow copy of this HeapMap.

		:return: The new HeapMap."""

		return type(self)(self.items())

	@unlocked
	@classmethod
	def fromkeys(cls, keys, value = None):
		"""Create a new WeakHeapMap with keys from iterable and values set to value.

		fromkeys() is a class method that returns a new HeapMap.

		:param iter iterable: The iterable that provides the keys.
		:param value: The value to use for all items. Defaults to None.
		:return: The new HeapMap."""

		return cls(dict.fromkeys(keys, value))

	def setdefault(self, key, default = None):
		"""If key is in the WeakHeapMap, return its value. If not, insert key with a
		value of default and return default.

		:param key: The key to look up and/or insert.
		:param default: The value to use if the key is not in the HeapMap.
		:return: Either the existing value or the default."""

		mapping = self.mapping
		fakenode = FakeKeyWeakHeapMapNode(id(key))
		try:
			ret = mapping[fakenode]
		except KeyError:
			ret_key = None
		else:
			ret_key = ret()
		if ret_key is None:
			self._setnode(ret, key, default)
			return default
		else:
			return ret.value

	def update(self, items = None, **kwargs):
		"""Update the dictionary with the key/value pairs from other, overwriting
		existing keys. Return None.

		update() accepts either another dictionary-like object and/or an iterable
		of key/value pairs (as tuples or other iterables of length two).
		If keyword arguments are specified, the HeapMap is then updated with
		those key/value pairs: h.update(red = 1, blue = 2).

		:param other: Add these items to the HeapMap.
		:type other: dict or iter(iter)
		:param dict kwargs: Add all keyword items to the HeapMap."""

		if self.heap:
			if items is None:
				pass
			elif hasattr(items, 'items'):
				for key, value in items.items():
					self._setitem(key, value)
			elif hasattr(items, 'keys'):
				for key in items.keys():
					self._setitem(key, items[key])
			else:
				for key, value in items:
					self._setitem(key, value)
			for key, value in kwargs.items():
				self._setitem(key, value)
		else:
			self._fill_initial(items, kwargs)

	def move_to_end(self, key, value = _no_value):
		"""move_to_end(self, key, [value])
		Move an existing key behind all other entries with the same value.

		If the entry already exists and a value is provided, update the entry
		with that value.

		If the entry does not exist and a value is provided, insert it.

		If the entry does not exist and a value is not provided, raise KeyError.

		:param key: The key for entry to add or update.
		:param value: The new value for the entry (optional)."""

		ret = self.mapping.get(FakeKeyWeakHeapMapNode(id(key)))
		if ret is None:
			if value is _no_value:
				raise KeyError(key)
			self._setnode(ret, key, value)
		else:
			old_serial = ret.serial
			serial = self._serial
			ret.serial = serial
			if value is _no_value:
				value = ret.value
			try:
				self._setnode(ret, key, value)
			except:
				ret.serial = old_serial
				raise
			self._serial = serial + 1

	@stub
	def _compare(self, a, b):
		pass

	@stub
	def reversed(self):
		"""Create a new WeakHeapMap that pops in the opposite direction, i.e., returns
		the largest item instead of the smallest or vice-versa. This is a shallow copy.

		:return: The new HeapMap."""

class MinWeakHeapMap(WeakHeapMap):
	"""__init__(items = None, **kwargs)

	WeakHeapMap variant that makes pop() return the smallest element.

	Initialized using the same interface as dict().

	:param items: Add this dict or an iterable of size 2 iterables
		to the HeapMap as initial values.
	:type items: dict or iter(iter)
	:param dict kwargs: Add all keyword items to the HeapMap as
		initial values."""

	def _compare(self, a, b):
		a_value = a.value
		b_value = b.value
		return a.serial < b.serial and not b_value < a_value or a_value < b_value

	@locked
	def reversed(self):
		return MaxWeakHeapMap(self)

class MaxWeakHeapMap(WeakHeapMap):
	"""__init__(items = None, **kwargs)

	WeakHeapMap variant that makes pop() return the largest element.

	Initialized using the same interface as dict().

	:param items: Add this dict or an iterable of size 2 iterables
		to the HeapMap as initial values.
	:type items: dict or iter(iter)
	:param dict kwargs: Add all keyword items to the HeapMap as
		initial values."""

	def _compare(self, a, b):
		a_value = a.value
		b_value = b.value
		return a.serial < b.serial and not a_value < b_value or b_value < a_value

	@locked
	def reversed(self):
		return MinWeakHeapMap(self)

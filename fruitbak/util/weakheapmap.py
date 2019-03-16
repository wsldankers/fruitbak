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

Entries with equal keys are extracted in insertion order. Iteration is in
insertion order if your Python dict iterates in insertion order (Python
>3.7).

Inconsistent results from the comparison functions will result in an
inconsistent heap.

This implementation keeps the heap consistent even if the comparison
functions of the items throw an exception. It is threadsafe."""

# TODO: implement move_to_end(key, value) using the serial
# TODO: use collections.abc.MutableMapping as base class
# TODO: use collections.abc.MappingView as base class

from .oo import stub
from .locking import lockingclass, unlocked, locked
from weakref import ref as weakref, KeyedRef
from sys import stderr
from collections.abc import Set
from traceback import print_exc

class FakeKeyWeakHeapMapNode:
	__slots__ = 'id', 'hash'

	def __init__(self, id):
		self.id = id
		self.hash = hash(id)

	def __hash__(self):
		return self.hash

	def __eq__(self, other):
		return self.id == other.id

class FakeValueWeakHeapMapNode:
	__slots__ = 'value', 'serial'

	def __init__(self, value, serial):
		self.value = value
		self.serial = serial

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

class WeakHeapMapValueView:
	__slots__ = 'mapping',

	def __init__(self, heapmap):
		self.mapping = heapmap.mapping

	def __iter__(self):
		for node in self.mapping:
			yield node.value

	def __contains__(self, value):
		for node in self.mapping:
			if node.value == value:
				return True
		return False

class WeakHeapMapItemView(Set):
	__slots__ = 'mapping',

	def __init__(self, heapmap):
		self.mapping = heapmap.mapping

	def __iter__(self):
		for node in heapmap.mapping:
			key = node()
			if key is not None:
				yield key, node.value

	def __contains__(self, item):
		key, value = item
		try:
			node = self.mapping[FakeKeyWeakHeapMapNode(id(key))]
		except KeyError:
			return False
		return node.value == value

# datatype: supports both extractmin and fetching by key
@lockingclass
class WeakHeapMap:
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
			ret += str(v.key) + " " + str(v.value) + " " + str(v.index) + "\n"
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

	def __setitem__(self, key, value):
		return self._setitem(key, value)

	@unlocked
	def _setitem(self, key, value):
		mapping = self.mapping
		heap = self.heap
		heap_len = len(heap)
		answers = []
		container = mapping.get(FakeKeyWeakHeapMapNode(id(key)))
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

	def pop(self, key = None):
		if key is None:
			while True:
				ret = self.heap[0]
				ret_key = ret()
				self._delnode(ret)
				if ret_key is not None:
					return ret.value
		else:
			ret = self.mapping[FakeKeyWeakHeapMapNode(id(key))]
			ret_key = ret()
			if ret_key is None:
				raise KeyError(key)
			self._delnode(ret)
			return ret.value

	def popkey(self, key = None):
		if key is None:
			ret = self.heap[0]
		else:
			ret = self.mapping[FakeKeyWeakHeapMapNode(id(key))]
		ret_key = ret()
		if ret_key is None:
			raise KeyError(key)
		self._delnode(ret)
		return ret_key

	def popitem(self, key = None):
		if key is None:
			ret = self.heap[0]
		else:
			ret = self.mapping[FakeKeyWeakHeapMapNode(id(key))]
		ret_key = ret()
		if ret_key is None:
			raise KeyError(key)
		self._delnode(ret)
		return ret_key, ret.value

	@unlocked
	def peek(self):
		return self.heap[0].value

	@unlocked
	def peekkey(self):
		return self.heap[0]()

	@unlocked
	def peekitem(self):
		item = self.heap[0]
		return item(), item.value

	@unlocked
	def keys(self):
		return self

	@unlocked
	def values(self):
		return WeakHeapMapValueView(self)

	@unlocked
	def items(self):
		return WeakHeapMapItemView(self)

	def clear(self):
		self.heap.clear()
		self.mapping.clear()
		self._serial = 0

	@unlocked
	def copy(self):
		return type(self)(self.items())

	@unlocked
	@classmethod
	def fromkeys(cls, keys, value = None):
		return cls(dict.fromkeys(keys, value))

	def setdefault(self, key, default = None):
		mapping = self.mapping
		fakenode = FakeKeyWeakHeapMapNode(id(key))
		try:
			ret = mapping[fakenode]
		except KeyError:
			ret_key = None
		else:
			ret_key = ret()
		if ret_key is None:
			self._setitem(key, default)
			return default
		else:
			return ret.value

	def update(self, items = None, **kwargs):
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

	@stub
	def _compare(self, a, b):
		pass

	@stub
	def reversed(self):
		pass

class MinWeakHeapMap(WeakHeapMap):
	def _compare(self, a, b):
		a_value = a.value
		b_value = b.value
		return a.serial < b.serial and not b_value < a_value or a_value < b_value

	@locked
	def reversed(self):
		return MaxWeakHeapMap(self)

class MaxWeakHeapMap(WeakHeapMap):
	def _compare(self, a, b):
		a_value = a.value
		b_value = b.value
		return a.serial < b.serial and not a_value < b_value or b_value < a_value

	@locked
	def reversed(self):
		return MinWeakHeapMap(self)

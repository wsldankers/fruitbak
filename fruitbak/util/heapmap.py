"""Heap queue object that also allows dictionary style access.

A variation on the heap queue that allows you to add and remove entries
as if it was a dictionary. The value of each item you add is used for
comparison (using the < operator) when maintaining the heap property.

It comes in two variants, one (MinHeapMap) that extracts the smallest
element when you call pop(), and one (MaxHeapMap) that extracts the
largest.

This implementation keeps the heap consistent even if the comparison
functions of the items throw an exception. It is threadsafe."""

# TODO: use a namedtuple for HeapMapNode
# TODO: add a counter field to that namedtuple just after key
#       that functions as a tie-breaker and makes the heap stable.

from .locking import lockingclass, unlocked
from collections.abc import Set

# Internal class that represents a node in the heapmap.
class HeapMapNode:
	__slots__ = ('key', 'value', 'index')
	def __init__(self, key, value, index):
		self.key = key
		self.value = value
		self.index = index

# Internal class that is returned for HeapMap.values()
class HeapMapValueView:
	__slots__ = ('mapping')

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

# Internal class that is returned for HeapMap.items()
class HeapMapItemView(Set):
	__slots__ = ('mapping')

	def __init__(self, heapmap):
		self.mapping = heapmap.mapping

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
class MinHeapMap:
	"""HeapMap variant that makes pop() return the smallest element."""
	def __init__(self, items = None, **kwargs):
		"""Initialize the HeapMap using the same interface as dict()."""
		heap = []
		mapping = {}

		if items is None:
			pass
		elif hasattr(items, 'items'):
			for key, value in items.items():
				mapping[key] = container = HeapMapNode(key, value, len(heap))
				heap.append(container)
		elif hasattr(items, 'keys'):
			for key in items.keys():
				mapping[key] = container = HeapMapNode(key, items[key], len(heap))
				heap.append(container)
		else:
			for key, value in items:
				mapping[key] = container = HeapMapNode(key, value, len(heap))
				heap.append(container)
		for key, value in kwargs.items():
			mapping[key] = container = HeapMapNode(key, value, len(heap))
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
					if self.compare(other_child.value, child.value):
						child = other_child
						child_index = other_child_index
				if self.compare(child.value, value):
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

			while index:
				parent_index = (index - 1) // 2
				parent = heap[parent_index]
				is_greater = bool(self.compare(value, parent.value))
				answers.append(is_greater)
				if is_greater:
					index = parent_index
				else:
					break

			index = heap_len
			container = HeapMapNode(key, value, index)
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

			while index:
				parent_index = (index - 1) // 2
				parent = heap[parent_index]
				is_greater = bool(self.compare(value, parent.value))
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
						is_greater = bool(self.compare(other_child.value, child.value))
						answers.append(is_greater)
						if is_greater:
							child = other_child
							child_index = other_child_index
					is_greater = bool(self.compare(child.value, value))
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
				is_greater = bool(self.compare(value, parent.value))
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
					is_greater = bool(self.compare(other_child.value, child.value))
					answers.append(is_greater)
					if is_greater:
						child = other_child
						child_index = other_child_index
				is_greater = bool(self.compare(child.value, value))
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

	@unlocked
	def compare(self, a, b):
		return a < b

	def pop(self, key = None):
		if key is None:
			ret = self.heap[0]
			del self[ret.key]
		else:
			ret = self[key]
			del self[key]
		return ret.value

	def popitem(self, key = None):
		if key is None:
			ret = self.heap[0]
			key = ret.key
		else:
			ret = self[key]
		del self[key]
		return key, ret.value

	@unlocked
	def peek(self):
		return self.heap[0].value

	@unlocked
	def peekitem(self):
		item = self.heap[0]
		return item.key, item.value

	@unlocked
	def keys(self):
		return self.mapping.keys()

	@unlocked
	def values(self):
		return HeapMapValueView(self)

	@unlocked
	def items(self):
		return HeapMapItemView(self)

	def clear(self):
		self.heap.clear()
		self.mapping.clear()

	@unlocked
	def reversed(self):
		return MaxHeapMap(self)

	@unlocked
	def copy(self):
		return type(self)(self.heap)

	@unlocked
	@classmethod
	def fromkeys(cls, keys, value = None):
		return cls(dict.fromkeys(keys, value))

	def setdefault(self, key, value = None):
		mapping = self.mapping
		if key in mapping:
			return mapping[key].value
		self[key] = value

	def update(self, items = None, **kwargs):
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

class MaxHeapMap(MinHeapMap):
	def compare(self, a, b):
		return b < a

	def reversed(self):
		return MinHeapMap(self)

from .oo import stub
from .locking import lockingclass, unlocked, locked
from weakref import ref as weakref
from sys import stderr
from collections.abc import Set
from traceback import print_exc

class FakeKeyWeakHeapMapNode:
	__slots__ = ('id', 'hash')
	def __init__(self, id):
		self.id = id
		self.hash = hash(id)

	def __hash__(self):
		return self.hash

	def __eq__(self, other):
		return self.id == other.id

class FakeValueWeakHeapMapNode:
	__slots__ = ('value', 'serial')
	def __init__(self, value, serial):
		self.value = value
		self.serial = serial

	def __lt__(self, other):
		self_value = self.value
		other_value = other.value
		return self.serial < other.serial and not other_value < self_value or self_value < other_value

class WeakHeapMapNode:
	__slots__ = ('weakkey', 'value', 'index', 'serial', 'id', 'hash')
	def __init__(self, key, value, index, serial, weakheapmap):
		key_id = id(key)

		def finalizer(weakkey):
			heapmap = weakheapmap()
			if heapmap is not None:
				fakenode = FakeKeyWeakHeapMapNode(key_id)
				try:
					heapmap._delnode(heapmap.mapping[fakenode])
				except KeyError:
					pass
				except:
					print_exc(file = stderr)

		self.weakkey = weakref(key, finalizer)
		self.value = value
		self.index = index
		self.serial = serial
		self.id = key_id
		self.hash = hash(key_id)

	def __hash__(self):
		return self.hash

	def __eq__(self, other):
		return self.id == other.id

	def __lt__(self, other):
		self_value = self.value
		other_value = other.value
		return self.serial < other.serial and not other_value < self_value or self_value < other_value

class WeakHeapMapValueView:
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

class WeakHeapMapItemView(Set):
	__slots__ = ('mapping')

	def __init__(self, heapmap):
		self.mapping = heapmap.mapping

	def __iter__(self):
		for node in heapmap.mapping:
			key = node.weakkey()
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
		heap = []
		mapping = {}
		serial = self._serial
		serial_increment = self._serial_increment
		weakself = weakref(self)

		if items is None:
			pass
		elif hasattr(items, 'items'):
			for key, value in items.items():
				container = WeakHeapMapNode(key, value, len(heap), serial, weakself)
				mapping[container] = container
				heap.append(container)
				serial += serial_increment
		elif hasattr(items, 'keys'):
			for key in items.keys():
				container = WeakHeapMapNode(key, items[key], len(heap), serial, weakself)
				mapping[container] = container
				heap.append(container)
				serial += serial_increment
		else:
			for key, value in items:
				container = WeakHeapMapNode(key, value, len(heap), serial, weakself)
				mapping[container] = container
				heap.append(container)
				serial += serial_increment
		for key, value in kwargs.items():
			container = WeakHeapMapNode(key, value, len(heap), serial, weakself)
			mapping[container] = container
			heap.append(container)
			serial += serial_increment

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
				
		self.heap = heap
		self.mapping = mapping
		self.weakself = weakself

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
			weakkey = node.weakkey
			key = node.weakkey()
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
		mapping = self.mapping
		heap = self.heap
		heap_len = len(heap)
		answers = []
		fakenode = FakeKeyWeakHeapMapNode(id(key))
		container = mapping.get(fakenode)
		if container is None:
			index = heap_len
			serial = self._serial
			container = WeakHeapMapNode(key, value, index, serial, self.weakself)

			while index:
				parent_index = (index - 1) // 2
				parent = heap[parent_index]
				is_greater = bool(self._compare(value, parent.value))
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

			self._serial = serial + self._serial_increment
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
		self._delnode(self.mapping[FakeKeyWeakHeapMapNode(id(key))])

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
			ret = self.heap[0]
			del self[ret.key]
			return ret.value
		else:
			ret = self[key]
			del self[key]
			return ret

	def popkey(self, key = None):
		if key is None:
			ret = self.heap[0]
		else:
			ret = self.mapping[FakeKeyWeakHeapMapNode(id(key))]
		key = ret.weakkey()
		del self[key]
		return key

	def popitem(self, key = None):
		if key is None:
			ret = self.heap[0]
		else:
			ret = self.mapping[FakeKeyWeakHeapMapNode(id(key))]
		key = ret.weakkey()
		del self[key]
		return key, ret.value

	@unlocked
	def peek(self):
		return self.heap[0].value

	@unlocked
	def peekkey(self):
		return self.heap[0].weakkey()

	@unlocked
	def peekitem(self):
		item = self.heap[0]
		return item.weakkey(), item.value

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

	@unlocked
	def copy(self):
		return type(self)(self.items())

	@unlocked
	@classmethod
	def fromkeys(cls, keys, value = None):
		return cls(dict.fromkeys(keys, value))

	def setdefault(self, key, value = None):
		mapping = self.mapping
		fakenode = FakeKeyWeakHeapMapNode(id(key))
		if fakenode in mapping:
			return mapping[fakenode].value
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

	@stub
	def _compare(self, a, b):
		pass

	@stub
	def reversed(self):
		pass

class MinWeakHeapMap(WeakHeapMap):
	_serial_increment = 1

	def _compare(self, a, b):
		return a < b

	@locked
	def reversed(self):
		return MaxWeakHeapMap(self)

class MaxWeakHeapMap(WeakHeapMap):
	_serial_increment = -1

	def _compare(self, a, b):
		return b < a

	@locked
	def reversed(self):
		return MinWeakHeapMap(self)

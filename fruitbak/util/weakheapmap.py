from .locking import lockingclass, unlocked
from weakref import ref as weakref
from sys import stderr
from collections.abc import Set
from traceback import print_exc

class FakeWeakHeapMapNode:
	__slots__ = ('id', 'hash')
	def __init__(self, id, hash):
		self.id = id
		self.hash = hash

	def __hash__(self):
		return self.hash

	def __eq__(self, other):
		return self.id == other.id

class WeakHeapMapNode:
	__slots__ = ('weakkey', 'value', 'index', 'id', 'hash')
	def __init__(self, key, value, index, weakheapmap):
		key_id = id(key)
		key_hash = hash(key)

		def finalizer(weakkey):
			heapmap = weakheapmap()
			if heapmap is not None:
				fakenode = FakeWeakHeapMapNode(key_id, key_hash)
				try:
					heapmap._delnode(heapmap.mapping[fakenode])
				except KeyError:
					pass
				except:
					print_exc(file = stderr)

		self.weakkey = weakref(key, finalizer)
		self.value = value
		self.index = index
		self.id = key_id
		self.hash = key_hash

	def __hash__(self):
		return self.hash

	def __eq__(self, other):
		return self.id == other.id

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
			node = self.mapping[FakeWeakHeapMapNode(id(key), hash(key))]
		except KeyError:
			return False
		return node.value == value

# datatype: supports both extractmin and fetching by key
@lockingclass
class MinWeakHeapMap:
	in_delete = 0

	def __init__(self, items = None, **kwargs):
		heap = []
		mapping = {}
		weakself = weakref(self)

		if items is None:
			pass
		elif hasattr(items, 'items'):
			for key, value in items.items():
				container = WeakHeapMapNode(key, value, len(heap), weakself)
				mapping[container] = container
				heap.append(container)
		elif hasattr(items, 'keys'):
			for key in items.keys():
				container = WeakHeapMapNode(key, items[key], len(heap), weakself)
				mapping[container] = container
				heap.append(container)
		else:
			for key, value in items:
				container = WeakHeapMapNode(key, value, len(heap), weakself)
				mapping[container] = container
				heap.append(container)
		for key, value in kwargs.items():
			container = WeakHeapMapNode(key, value, len(heap), weakself)
			mapping[container] = container
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
		return FakeWeakHeapMapNode(id(key), hash(key)) in self.mapping

	@unlocked
	def __len__(self):
		return len(self.heap)

	@unlocked
	def __getitem__(self, key):
		return self.mapping[FakeWeakHeapMapNode(id(key), hash(key))].value

	def __setitem__(self, key, value):
		mapping = self.mapping
		heap = self.heap
		heap_len = len(heap)
		answers = []
		fakenode = FakeWeakHeapMapNode(id(key), hash(key))
		container = mapping.get(fakenode)
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
			container = WeakHeapMapNode(key, value, index, self.weakself)
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
		self._delnode(self.mapping[FakeWeakHeapMapNode(id(key), hash(key))])

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
	def reversed(self):
		return MaxWeakHeapMap(self)

	@unlocked
	def copy(self):
		return type(self)(self.items())

	@unlocked
	@classmethod
	def fromkeys(cls, keys, value = None):
		return cls(dict.fromkeys(keys, value))

	def setdefault(self, key, value = None):
		mapping = self.mapping
		fakenode = FakeWeakHeapMapNode(id(key), hash(key))
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

class MaxWeakHeapMap(MinWeakHeapMap):
	def compare(self, a, b):
		return b < a

	def reversed(self):
		return MinWeakHeapMap(self)

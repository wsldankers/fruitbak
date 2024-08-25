"""Heap queue object that also allows dictionary style access.

A variation on the heap queue that allows you to add and remove entries as
if it was a dictionary. The value of each item you add is used for
comparison when maintaining the heap property. This comparison is done
using the < operator exclusively, so for custom value objects you only need
to implement __lt__().

It comes in two variants, one (MinHeapMap) that extracts the smallest
element when you call pop(), and one (MaxHeapMap) that extracts the
largest.

Entries with equal values are extracted in insertion order. Iteration is in
insertion order if your Python's dict implementation iterates in insertion
order (Python 3.7+).

Inconsistent results from the comparison functions will result in an
inconsistent heap. Comparison functions with side effects cause undefined
behavior if these side effects affect the HeapMap.

This implementation keeps the heap consistent even if the comparison
functions of the items throw an exception. It is threadsafe."""

from collections.abc import ItemsView, MutableMapping, ValuesView

from .locking import locked, lockingclass, unlocked
from .oo import stub


class _NoValue:
    pass


_no_value = _NoValue()


# Internal class that represents a node in the heapmap.
class HeapMapNode:
    __slots__ = 'key', 'value', 'index', 'serial'

    def __init__(self, key, value, index, serial):
        self.key = key
        self.value = value
        self.index = index
        self.serial = serial

    def __repr__(self):
        return 'HeapMapNode(key = %r, value = %r, index = %d, serial = %d)' % (
            self.key,
            self.value,
            self.index,
            self.serial,
        )


# Internal class that is returned for HeapMap.values()
class HeapMapValuesView(ValuesView):
    def __init__(self, heapmap):
        super().__init__(heapmap.mapping)

    def __iter__(self):
        for node in self._mapping:
            yield node.value

    def __contains__(self, value):
        for node in self._mapping:
            if node.value == value:
                return True
        return False


# Internal class that is returned for HeapMap.items()
class HeapMapItemsView(ItemsView):
    def __init__(self, heapmap):
        super().__init__(heapmap.mapping)

    def __iter__(self):
        for node in self._mapping:
            yield node.key, node.value

    def __contains__(self, item):
        key, value = item
        try:
            node = self.mapping[key]
        except KeyError:
            return False
        v = node.value
        return v is value or v == value


# datatype: supports both extractmax and fetching by key
@lockingclass
class HeapMap(MutableMapping):
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

    _serial = 0

    def __init__(self, items=None, **kwargs):
        self.heap = []
        self.mapping = {}

        self._fill_initial(items, kwargs)

    @unlocked
    def _fill_initial(self, items, kwargs):
        heap = self.heap
        mapping = self.mapping

        assert not heap

        heap_len = 0

        try:
            if items is None:
                pass
            elif hasattr(items, 'items'):
                for key, value in items.items():
                    mapping[key] = container = HeapMapNode(
                        key, value, len(heap), heap_len
                    )
                    heap.append(container)
                    heap_len += 1
            elif hasattr(items, 'keys'):
                for key in items.keys():
                    mapping[key] = container = HeapMapNode(
                        key, items[key], len(heap), heap_len
                    )
                    heap.append(container)
                    heap_len += 1
            else:
                for key, value in items:
                    mapping[key] = container = HeapMapNode(
                        key, value, len(heap), heap_len
                    )
                    heap.append(container)
                    heap_len += 1
            for key, value in kwargs.items():
                mapping[key] = container = HeapMapNode(key, value, len(heap), heap_len)
                heap.append(container)
                heap_len += 1

            # the following loop runs in amortized O(n) time:
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
        for k, v in self.mapping.items():
            ret += (
                str(v.key)
                + " "
                + str(v.value)
                + " "
                + str(v.index)
                + " ("
                + str(k)
                + ")\n"
            )
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

    def get(key, default=None):
        """Return the value for `key` if `key` is in the mapping, else `default`.
        If `default` is not given, it defaults to `None`, so that this method never
        raises a `KeyError`.

        :param key: The key to look up.
        :param default: The value to return if `key` was not found.
        :return: The value belonging to `key` or `default` if it was not found."""

        mapping = self.mapping
        try:
            node = mapping[key]
        except KeyError:
            pass
        else:
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
        self._setnode(self.mapping.get(key), key, value)

    @unlocked
    def _setnode(self, container, key, value):
        mapping = self.mapping
        heap = self.heap
        heap_len = len(heap)
        answers = []
        if container is None:
            index = heap_len
            serial = self._serial
            container = HeapMapNode(key, value, index, serial)

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

            self._serial = serial + 1
            container.index = index
            heap[index] = container
        else:
            index = container.index
            comparison = HeapMapNode(
                container.key, container.serial, value, container.index
            )

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
        self._delnode(self.mapping[key])

    def remove(self, key):
        """Remove the item corresponding to the specified key.
        Raises `KeyError` if `key` is not contained in the set.

        :param key: The key to remove."""

        self._delnode(self.mapping[key])

    def discard(self, key):
        """Remove the item corresponding to the specified key, if it is present.

        :param key: The key to remove."""

        mapping = self.mapping
        try:
            node = mapping[key]
        except KeyError:
            pass
        else:
            self._delnode(node)

    def _delnode(self, victim):
        index = victim.index

        mapping = self.mapping
        heap = self.heap
        heap_len = len(heap) - 1

        if index == heap_len:
            heap.pop()
            del mapping[victim.key]
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

        del mapping[victim.key]
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

    def pop(self, key=None):
        """Remove and return the value in the heap corresponding to the specified key.
        If key is absent or None, remove and return the smallest/largest value in the heap.

        :param key: The key of the value to remove and return.
        :return: The value corresponding to key."""

        return self.popitem(key)[1]

    def popkey(self, key=None):
        """Remove and return the key in the heap equal to the specified key.
        If key is absent or None, remove and return the smallest/largest value in the heap.

        :param key: The key of the value to remove and return.
        :return: The value corresponding to key."""

        return self.popitem(key)[0]

    def popitem(self, key=None):
        """Remove and return a tuple of the key and the value in the heap
        corresponding to the specified key. If key is absent or None, remove and
        return the smallest/largest item in the heap.

        :param key: The key of the item to remove and return.
        :return: A tuple of (key, value) corresponding to key."""

        if key is None:
            ret = self.heap[0]
            key = ret.key
        else:
            ret = self.mapping[key]
        self._delnode(ret)
        return key, ret.value

    @unlocked
    def peek(self, key=None):
        """Return the value in the heap corresponding to the specified `key`. If
        `key` is absent or `None`, return the smallest/largest value in the heap.

        :param key: The key of the value to return.
        :return: The value corresponding to key."""

        return self.peekitem(key)[1]

    @unlocked
    def peekkey(self, key=None):
        """Return the key in the heap corresponding to the specified `key`. If
        `key` is absent or `None`, return the key of the smallest/largest value in
        the heap.

        :param key: The key to look up and return.
        :return: The value corresponding to key."""

        return self.peekitem(key)[0]

    @unlocked
    def peekitem(self, key=None):
        """Return a tuple of the key and the value in the heap corresponding to the
        specified `key`. If `key` is absent or `None`, return the item in the heap
        with the smallest/largest value.

        :param key: The key of the item to return.
        :return: A tuple of (key, value) corresponding to key."""

        if key is None:
            item = self.heap[0]
        else:
            item = self.mapping[key]
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

        return HeapMapValuesView(self)

    @unlocked
    def items(self):
        """Return a view of the keys and values.

        :return: A view of the keys and values of the mapping, in a tuple each."""

        return HeapMapItemsView(self)

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
    def fromkeys(cls, iterable, value=None):
        """Create a new HeapMap with keys from iterable and values set to value.

        fromkeys() is a class method that returns a new HeapMap.

        :param iter iterable: The iterable that provides the keys.
        :param value: The value to use for all items. Defaults to None.
        :return: The new HeapMap."""

        return cls(dict.fromkeys(iterable, value))

    def setdefault(self, key, default=None):
        """If key is in the HeapMap, return its value. If not, insert key with a
        value of default and return default.

        :param key: The key to look up and/or insert.
        :param default: The value to use if the key is not in the HeapMap.
        :return: Either the existing value or the default."""

        mapping = self.mapping
        try:
            node = mapping[key]
        except KeyError:
            pass
        else:
            return node.value
        self._setitem(key, default)
        return default

    def update(self, other=None, **kwargs):
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

    def move_to_end(self, key, value=_no_value):
        """move_to_end(self, key, [value])
        Move an existing key behind all other entries with the same value.

        If the entry already exists and a value is provided, update the entry
        with that value.

        If the entry does not exist and a value is provided, insert it.

        If the entry does not exist and a value is not provided, raise KeyError.

        :param key: The key for entry to add or update.
        :param value: The new value for the entry (optional)."""

        ret = self.mapping.get(key)
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

    def _compare(self, a, b):
        a_value = a.value
        b_value = b.value
        return a.serial < b.serial and not b_value < a_value or a_value < b_value

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

    def _compare(self, a, b):
        a_value = a.value
        b_value = b.value
        return a.serial < b.serial and not a_value < b_value or b_value < a_value

    @locked
    def reversed(self):
        return MinHeapMap(self)

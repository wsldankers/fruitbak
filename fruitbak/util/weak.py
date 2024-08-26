"""Collection of weakref-related utility functions

This "collection" of weakref-related utility functions currently only
contains one class (weakproperty)."""

import weakref
from functools import wraps


class weakproperty(property):
    """A property that keeps a weak reference.

    Unlike a normal property it doesn't call functions for
    getting/setting/deleting but just stores the value in the object's
    dictionary.

    The function you'd decorate with this property is used as an
    initializer that is called when the attribute is retrieved and the
    property was either never set, it was deleted, or the weak reference
    was lost.

    The name of the supplied initializer function is also used as the key
    for the object's dictionary.

    Because it is implemented as a property with preset get/set/delete
    functions, any attempts to change the get/set/delete functions will
    break it.

    :param function f: Called with self as an argument when the property
            is dereferenced and no value was available in the dictionary."""

    def __init__(prop, f):
        name = f.__name__

        @wraps(f)
        def getter(self):
            dict = vars(self)
            try:
                weak = dict[name]
            except KeyError:
                pass
            else:
                value = weak()
                if value is not None:
                    return value
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

        @wraps(f)
        def setter(self, value):
            dict = vars(self)

            def unsetter(weak):
                try:
                    if dict[name] is weak:
                        del dict[name]
                except KeyError:
                    pass

            dict[name] = weakref.ref(value, unsetter)

        @wraps(f)
        def deleter(self):
            del vars(self)[name]

        super().__init__(getter, setter, deleter)

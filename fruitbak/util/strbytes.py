"""Functions to convert between str and bytes objects. Typically used to
handle filenames that may not be in the same encoding as the local
filesystem, so they use a generic UTF-8 with surrogateescape encoding
scheme.

See `the Python 3 glossary on ‘bytes-like object’
<https://docs.python.org/3/glossary.html#term-bytes-like-object>`_ for
details on what it means for an object to be bytes-like."""

from pathlib import PurePath


def is_byteslike(obj):
    """Test whether `obj` is a bytes-like object.

    :param obj: The object to test
    :return: Whether the object is bytes-like.
    :rtype: bool"""

    try:
        memoryview(obj)
    except TypeError:
        return False
    else:
        return True


def ensure_bytes(obj):
    """Encode str obj to UTF-8 encoding with 'surrogateescape' error handler,
    return bytes-like ojects as bytes objects. Can handle Path objects.

    :param obj: The object to convert
    :type obj: byteslike or str or Path
    :return: The potentially converted input
    :rtype: bytes"""

    if isinstance(obj, bytes):
        return obj

    try:
        memoryview(obj)
    except TypeError:
        pass
    else:
        return bytes(obj)

    if isinstance(obj, PurePath):
        return bytes(obj)

    try:
        return bytes(obj, 'UTF-8', 'surrogateescape')
    except TypeError:
        pass

    raise TypeError("cannot convert '%s' object to bytes" % (type(obj).__name__))


def ensure_byteslike(obj):
    """Encode str obj to UTF-8 encoding with 'surrogateescape' error handler,
    return bytes-like ojects unchanged. Can handle Path objects.

    :param obj: The object to convert
    :type obj: byteslike or str or Path
    :return: The potentially converted input
    :rtype: byteslike"""

    try:
        memoryview(obj)
    except TypeError:
        pass
    else:
        return obj

    if isinstance(obj, PurePath):
        return bytes(obj)

    try:
        return bytes(obj, 'UTF-8', 'surrogateescape')
    except TypeError:
        pass

    raise TypeError("cannot convert '%s' object to bytes" % (type(obj).__name__))


def ensure_str(obj):
    """
    Decode bytes-like obj from UTF-8 encoding with 'surrogateescape' error handler,
    return str objects unchanged. Can handle Path objects.

    :param obj: The object to convert
    :type obj: byteslike or str or Path
    :return: The potentially converted input
    :rtype: str"""

    if isinstance(obj, str):
        return obj

    if isinstance(obj, PurePath):
        return str(obj)

    try:
        return str(obj, 'UTF-8', 'surrogateescape')
    except TypeError:
        pass

    raise TypeError("cannot convert '%s' object to str" % type(obj).__name__)

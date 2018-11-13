from pathlib import PurePath

def ensure_bytes(obj):
	"""
	Encode str obj to UTF-8 encoding with 'surrogateescape' error handler,
	return bytes-like ojects as bytes objects. Can handle Path objects.
	"""
	if isinstance(obj, str):
		return obj.encode(errors = 'surrogateescape')
	if isinstance(obj, bytes):
		return obj
	return obj.__bytes__()

def ensure_byteslike(obj):
	"""
	Encode str obj to UTF-8 encoding with 'surrogateescape' error handler,
	return bytes-like ojects unchanged. Can handle Path objects.
	"""
	if isinstance(obj, str):
		return obj.encode(errors = 'surrogateescape')
	try:
		memoryview(obj)
	except TypeError:
		pass
	else:
		return obj
	return obj.__bytes__()

def ensure_str(obj):
	"""
	Decode bytes-like obj from UTF-8 encoding with 'surrogateescape' error handler,
	return str objects unchanged. Can handle Path objects.
	"""
	try:
		return str(obj, errors = 'surrogateescape')
	except TypeError:
		pass
	if isinstance(obj, str):
		return obj
	if isinstance(obj, Path):
		return str(Path)
	raise TypeError("expect byteslike, str or Path, not %s" % type(obj).__name__)

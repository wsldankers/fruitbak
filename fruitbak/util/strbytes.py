def ensure_bytes(obj):
	"""
	Encode str obj to UTF-8 encoding with 'surrogateescape' error handler,
	return bytes-like ojects as bytes objects.
	"""
	if isinstance(obj, str):
		return obj.encode(errors = 'surrogateescape')
	elif isinstance(obj, bytes):
		return obj
	else:
		return bytes(obj)

def ensure_byteslike(obj):
	"""
	Encode str obj to UTF-8 encoding with 'surrogateescape' error handler,
	return bytes-like ojects unchanged.
	"""
	if isinstance(obj, str):
		return obj.encode(errors = 'surrogateescape')
	try:
		memoryview(obj)
	except:
		raise TypeError("expect bytes or str, not %s" % type(obj).__name__) from None
	else:
		return obj

def ensure_str(obj):
	"""
	Decode bytes-like obj from UTF-8 encoding with 'surrogateescape' error handler,
	return str ojects unchanged.
	"""
	if isinstance(obj, str):
		return obj
	elif not isinstance(obj, bytes):
		obj = bytes(obj)
	return obj.decode(errors = 'surrogateescape')

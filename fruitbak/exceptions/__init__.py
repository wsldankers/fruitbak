class FruitbakException(Exception):
	"""Abstract base class for Fruitbak exceptions"""

class NotFoundError(FruitbakException):
	"""
	Abstract base class for Fruitbak exceptions that indicate
	something was not found.
	"""

class HostNotFoundError(NotFoundError, KeyError):
	"""Indicates that a requested host was not found"""

	def __str__(self):
		return f"Host '{self.args[0]}' not found"

class BackupNotFoundError(NotFoundError, IndexError):
	"""Indicates that a requested backup was not found"""

	def __str__(self):
		return f"Backup '{self.args[0]}' not found"

class ShareNotFoundError(NotFoundError, KeyError):
	"""Indicates that a requested share was not found"""

	def __str__(self):
		return f"Share '{self.args[0]}' not found"

from fruitbak.pool.storage.filesystem import Filesystem
try:
	from fruitbak.pool.storage.filesystem import LinuxFilesystem
except ImportError:
	pass

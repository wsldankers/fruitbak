from fruitbak.pool.handler import Handler

class Storage(Handler):
	pass

from fruitbak.pool.storage.filesystem import Filesystem
try:
	from fruitbak.pool.storage.filesystem import LinuxFilesystem
except ImportError:
	pass
from fruitbak.pool.storage.lmdb import LMDB

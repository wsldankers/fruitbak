"""Top-level object for a Fruitbak installation"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.host import Host
from pathlib import Path
import os

class Fruitbak(Clarity):
	"""Top-level object for a Fruitbak installation.

	For each Fruitbak installation you should instantiate a Fruitbak
	object as a starting point for accessing its configuration and
	backed up hosts.
	"""

	@initializer
	def rootdir(self):
		return Path(self.cfg['rootdir'])

	@initializer
	def hostdir(self):
		return self.rootdir / 'host'

	@initializer
	def hosts(self):
		hosts = []
		for entry in self.hostdir.iterdir():
			if entry.is_dir() and not entry.name.startswith('.'):
				hosts.append(Host(name = entry.name, fruitbak = self))
		return hosts

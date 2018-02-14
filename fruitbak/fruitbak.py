"""Top-level object for a Fruitbak installation"""

from fruitbak.util.clarity import Clarity, initializer
from fruitbak.host import Host
from pathlib import Path
from urllib.parse import quote, unquote
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
				hosts.append(Host(fruitbak = self, hostdir = entry))
		return hosts

	def name_to_path(self, name):
		return Path(quote(name[0], errors = 'strict', safe = '+=_,%@')
			+ quote(name[1:], errors = 'strict', safe = '+=_,%@.-'))

	def path_to_name(self, path):
		return unquote(str(path), errors = 'strict')

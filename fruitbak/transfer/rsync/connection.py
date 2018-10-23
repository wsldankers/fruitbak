from fruitbak.util import Clarity, initializer
from enum import Enum
from select import poll, POLLIN, POLLOUT, POLLHUP, POLLERR
from threading import Thread, Condition
from os import read
from collections import deque

class MessageCodes(Enum):
	MSG_DATA = 0
    MSG_ERROR_XFER = 1
    MSG_INFO = 2
    MSG_ERROR = 3
    MSG_WARNING = 4
    MSG_ERROR_SOCKET = 5
    MSG_LOG = 6
    MSG_CLIENT = 7
    MSG_ERROR_UTF8 = 8
    MSG_REDO = 9
    MSG_FLIST = 20
    MSG_FLIST_EOF = 21
    MSG_IO_ERROR = 22
    MSG_NOOP = 42
    MSG_DONE = 86
    MSG_SUCCESS = 100
    MSG_DELETED = 101
    MSG_NO_SEND = 102

class RsyncConnection(Clarity):
	timeout = 10

	@initializer
	def pid(self):
		raise RuntimeError("uninitialized property 'pid'")

	@initializer
	def poll(self):
		p = poll()
		p.register(self.in_fd, POLLIN)
		return p

	@initializer
	def in_fd(self):
		raise RuntimeError("uninitialized property 'in_fd'")

	@initializer
	def in_buf(self):
		return []

	@initializer
	def out_fd(self):
		raise RuntimeError("uninitialized property 'out_fd'")

	@initializer
	def out_buf(self):
		return []

	def do_io(self):
		poll = self.poll
		in_fd = self.in_fd
		out_fd = self.out_fd
		in_buf = self.in_buf
		out_buf = self.out_buf
		if out_buf:
			poll.register(out_fd, POLLOUT)
		else:
			poll.register(out_fd, 0)

		for fd, event in poll.poll(self.timeout * 1000):
			

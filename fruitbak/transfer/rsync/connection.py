from fruitbak.util import Clarity, initializer
from enum import Enum
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
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
	@initializer
	def pid(self):
		raise RuntimeError("uninitialized property 'pid'")

	@initializer
	def selector(self):
		return DefaultSelector()

	@initializer
	def in_fd(self):
		raise RuntimeError("uninitialized property 'in_fd'")

	@initializer
	def in_buf(self):
		return []

	@initializer
	def in_eof(self):
		return []

	@initializer
	def in_cond(self):
		return Condition()

	@initializer
	def in_thread(self):
		fd = self.in_fd
		cond = self.in_cond
		buf = self.in_buf
		eof = self.in_eof
		def run():
			with DefaultSelector() as selector:
				selector.register(fd, EVENT_READ)
				while True:
					if selector.select():
						b = read(fd, 65536)
						with cond:
							if b:
								buf.append(b)
							else:
								eof.append(None)
							cond.notify_all()
					if not b:
						break
		return Thread(target = run, daemon = True)

	@initializer
	def out_fd(self):
		raise RuntimeError("uninitialized property 'out_fd'")

	@initializer
	def out_buf(self):
		return []

	@initializer
	def out_eof(self):
		return []

	@initializer
	def out_cond(self):
		return Condition()

	@initializer
	def out_thread(self):
		fd = self.out_fd
		cond = self.out_cond
		buf = self.out_buf
		eof = self.out_eof
		deq = deque()
		def run():
			with DefaultSelector() as selector:
				selector.register(fd, EVENT_WRITE)
				while not eof:
					with cond:
						while not eof and not buf:
							cond.wait()
					if eof:
						return
					if selector.select():
						while not eof:
							with cond:
								deq.extend(buf)
								buf.clear()
							if not deq:
								break
							n = writev(fd, deq)
							while n > 0:
								first = deq[0]
								first_len = len(first)
								if n < first_len:
									deq[0] = first[n:]
									n = 0
								else:
									deq.popleft()
									n -= first_len
		return Thread(target = run, daemon = True)



from fruitbak.util import Clarity, initializer
from fruitbak.transfer.rsync.constants import *

from enum import Enum
from select import poll, POLLIN, POLLOUT, POLLHUP, POLLERR
from threading import Thread, Condition
from os import read, writev
from collections import deque
from subprocess import Popen, PIPE
from struct import Struct

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

multiplex_header = Struct('<L')
multiplex_header_size = multiplex_header.size
multiplex_header_pack = multiplex_header.pack
multiplex_header_unpack = multiplex_header.unpack

class BucketBrigade:
	@initializer
	def _deque(self):
		return deque()

	@initializer
	def _len(self):
		return sum(map(len, self._deque))

	def write(self, buf):
		assert buf
		self._len += len(buf)
		self._deque.append(buf)

	def read(self, amount = None):
		deq = self._deque
		if amount is None:
			try:
				return deq.popleft()
			except IndexError:
				return b''
		else:
			curlen = self._len
			# if we fail half way, self._len will be recalculated
			del self._len
			amount = min(amount, curlen)
			curlen -= amount
			buf = []

			while amount > 0:
				first = deq.popleft()
				first_len = len(first)
				if first_len > amount:
					#first = memoryview(first)
					deq.appendleft(first[amount:])
					buf.append(first[:amount])
					break
				else:
					buf.append(first)
					amount -= first_len

			self._len = curlen
			return b''.join(buf)

	def __len__(self):
		return self._len

	def __bool__(self):
		return self._len > 0

class RsyncConnection(Clarity):
	# we use ns for units
	timeout = 10000000000

	@initializer
	def popen(self):
		return Popen(
			'rsync --server --sender -lHogDtpre.iLsf /etc/network'.split(' '),
			executable = '/usr/bin/rsync',
			stdin = PIPE,
			stdout = PIPE,
			bufsize = 0,
		)

	@initializer
	def pid(self):
		return self.popen.pid

	@initializer
	def poll(self):
		p = poll()
		p.register(self.in_fd, POLLIN)
		return p

	@initializer
	def in_fd(self):
		fd = self.popen.stdout.fileno()
		set_blocking(fd, False)
		return fd

	@initializer
	def in_buf(self):
		return BucketBrigade()

	multiplex_in = None

	def start_multiplex_in(self):
		self.multiplex_in = True

	def stop_multiplex_in(self):
		self.multiplex_in = False

	@initializer
	def out_fd(self):
		fd = self.popen.stdin.fileno()
		set_blocking(fd, False)
		return fd

	@initializer
	def out_buf(self):
		return deque()

	multiplex_out = None

	def start_multiplex_out(self):
		self.multiplex_out = True

	def stop_multiplex_out(self):
		self.multiplex_out = False

	def do_io(self):
		in_fd = self.in_fd
		out_fd = self.out_fd
		in_buf = self.in_buf
		in_buf_len = self.in_buf_len
		out_buf = self.out_buf

		poll = self.poll
		if out_buf:
			poll.register(out_fd, POLLOUT)
		else:
			poll.register(out_fd, 0)

		for fd, event in poll.poll(self.timeout / 1000000):
			if event & (POLLERR | POLLHUP):
				raise RuntimeError("rsync subprocess ended unexpectedly")
			if event & POLLIN:
				assert fd == in_fd
				buf = read(fd, 65536)
				if not buf:
					raise RuntimeError("rsync subprocess ended unexpectedly")
				in_buf.write(buf)
			if event & POLLOUT:
				assert fd == out_fd
				r = writev(fd, out_buf)
				while r > 0:
					first = out_buf[0]
					first_len = len(first)
					if r < first_len:
						out_buf[0] = first[n:]
						break
					else:
						out_buf.popleft()
						r -= first_len

		active_channel = self.active_channel
		active_channel_len = self.active_channel_len

		try:
			in_channels = self.in_channels
			stream_handlers = self.stream_handlers

			while in_buf:
				if active_channel is not None:
					buf = in_buf.read(active_channel_len)
					active_channel_len -= len(buf)

					try:
						stream_handler = stream_handlers[active_channel]
					except KeyError:
						try:
							in_channel = in_channels[active_channel]
						except KeyError:
							in_channel = BucketBrigade()
							in_channels[active_channel] = in_channel
						in_channel.write(buf)
					else:
						try:
							in_channel = in_channels.pop(active_channel)
						except KeyError:
							stream_handler(buf)
						else:
							in_channel.write(buf)
							stream_handler(in_channel.read(len(in_channel)))

					if not active_channel_len:
						active_channel is None

				if not self.multiplex_in:
					break

				if active_channel is None:
					if len(in_buf) < multiplex_header_size:
						break
					header, = multiplex_header_unpack(in_buf.read(multiplex_header_size))
					active_channel = (header >> 24) - MPLEX_BASE
					active_channel_len = header & 0xFFFFFF

		finally:
			self.active_channel = active_channel
			self.active_channel_len = active_channel_len

	def send_bytes_raw(self, data):
		self.out_buf.append(data)

	def send_datagram(self, code, data):
		data_len = len(data)
		if data_len > 0xFFFFFF:
			data = memoryview(data)
			for off in range(0, data_len, 0xFFFFFF):
				self.send_datagram(code, data[off:off + 0xFFFFFF])
		else:
			self.send_bytes_raw(multiplex_header_pack((code + MPLEX_BASE << 24) + data_len))
			self.send_bytes_raw(data)

	def resolve_code(self, code = msgcodes.MSG_DATA):
		return code.value

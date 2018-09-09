from numbers import Number
from os import linesep

def tabulate(rows, *, headings = None, alignment = (), tablefmt = None, linesep = linesep, columnsep = '  '):
	if headings is not None:
		rows = (headings, *rows)
	else:
		rows = tuple(rows)

	widths = []
	for row in rows:
		for i, column in enumerate(row):
			l = len(str(column))
			if i >= len(widths):
				widths.append(l)
			else:
				if widths[i] < l:
					widths[i] = l

	output = []
	for j, row in enumerate(rows):
		if j:
			output.append(linesep)
		for i, column in enumerate(row):
			if i:
				output.append(columnsep)
			w = widths[i]
			try:
				align = alignment[i]
			except (KeyError, IndexError):
				align = None
			if align is None:
				align = isinstance(column, Number)
			output.append(str(column).rjust(w) if align else str(column).ljust(w))

	return ''.join(output)

from numbers import Number

def tabulate(rows, *, headings = None, alignment = (), tablefmt = None):
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
			output.append("\n")
		for i, column in enumerate(row):
			if i:
				output.append('  ')
			w = widths[i]
			try:
				align = alignment[i]
			except KeyError:
				align = None
			except IndexError:
				align = None
			if align is None:
				if not isinstance(column, Number):
					w = -w
			elif align:
				w = -w
			output.append(('%' + str(w) + 's') % str(column))

	return ''.join(output)

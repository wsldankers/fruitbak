from numbers import Number
from os import linesep

def tabulate(rows, *, headings = None, alignment = (), tablefmt = None, linesep = linesep, columnsep = '  '):
	if headings is not None:
		rows = (headings, *rows)

	widths = []
	string_rows = []
	for row in rows:
		string_columns = []
		for i, column in enumerate(row):
			s = str(column)
			string_columns.append((column, s))
			l = len(s)
			if i >= len(widths):
				widths.append(l)
			else:
				if widths[i] < l:
					widths[i] = l
		string_rows.append(string_columns)

	output_rows = []
	for row in string_rows:
		output_columns = []
		for i, (column, s) in enumerate(row):
			try:
				align = alignment[i]
			except (KeyError, IndexError):
				align = None
			if align is None:
				align = isinstance(column, Number)

			w = widths[i]
			if align:
				formatted = s.rjust(w)
			else:
				formatted = s.ljust(w)

			output_columns.append(formatted)

		output_rows.append(columnsep.join(output_columns).rstrip())

	return linesep.join(output_rows)

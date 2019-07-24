"""A function to create a table-like textual representation of an
iterable of iterables of strings."""

from numbers import Number
from os import linesep

def tabulate(rows, *, headings = None, alignment = (), tablefmt = None, linesep = linesep, columnsep = '  '):
	"""Concatenate an iterable of iterables of strings such that it looks like
	a table. Return the table as a str.

	Lines or headers do not need to have the same number of columns.

	If the `headings` parameter is present and not None, it should be an
	iterable of strings that will be used as the first line.

	The `alignment` parameter is an optional iterable of boolean and/or None
	values that indicate whether a column is left-justified or right-justified.
	None indicates the default behavior, which is to right-justify instances of
	Number and left-align everything else.

	If a line contains more elements than the `alignment` parameter, it is
	assumed that the extra cells will formatted as if None was specified. The
	default is an empty iterable (so all cells will be justified depending on
	their type).

	The `tablefmt` parameter is provided for compatibility with python-tabulate
	and is ignored.

	:param iter(iter(str)) rows: Rows of lines of strings that form the cell data.
	:param iter(str) headings: An optional line of strings that form the column headings. If this parameter is absent or None, no heading will be rendered.
	:param alignment: How to justify each cell. Defaults to an empty iterator.
	:type alignment: iter(bool or None)
	:param tablefmt: Ignored.
	:param str linesep: The separator between lines. No `linesep` is appended after the last line. Defaults to `os.linesep`.
	:param str columnsep: The separator between columns. Defaults to two spaces.
	:return: The formatted table.
	:rtype: str"""

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
			except LookupError:
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

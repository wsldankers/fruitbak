"""Utilities for doing arithmetic on time values (expressed as nanoseconds
since 1970-01-01 00:00:00 UTC).

This module also exports `time_ns()`, either from the `time` module or a
polyfill if that function is not available."""

from calendar import timegm as _timegm
from fractions import Fraction as _Fraction
from re import IGNORECASE as _IGNORECASE, compile as _regcomp
from time import localtime as _localtime, mktime as _mktime, strftime as _strftime

try:
    from time import time_ns
except ImportError:
    from time import time as _time

    def time_ns():
        return int(_time() * 1000000000.0)


def format_time(t):
    """Format a timestamp (expressed as nanoseconds since 1970-01-01 00:00:00
    UTC) into human-readable syntax with seconds granularity.

    :param int t: the timestamp

    :return: A human-readable string.
    :rtype: str"""

    return _strftime('%Y-%m-%d %H:%M:%S', _localtime(t // 1000000000))


def format_interval(t):
    """Format an interval (expressed as nanoseconds) into human-readable syntax.

    :param int t: the interval

    :return: A human-readable string.
    :rtype: str"""

    if t >= 86400000000000:
        d, s = divmod(t, 86400_000_000_000)
        return '%dd%dh' % (d, s // 3600_000_000_000)
    elif t >= 3600000000000:
        h, s = divmod(t, 3600_000_000_000)
        return '%dh%dm' % (h, s // 60_000_000_000)
    elif t >= 60000000000:
        m, s = divmod(t, 60_000_000_000)
        return '%dm%ds' % (m, s // 1_000_000_000)
    else:
        s, ns = divmod(t, 1_000_000_000)
        if ns:
            return '%d.%02ds' % (s, ns // 10_000_000)
        else:
            return '%ds' % s


_parse_interval_re = _regcomp(r'\s*(\d+)\s*([smhdwlqy]|[mun]s)\s*', _IGNORECASE)

_parse_interval_units = dict(
    ns=1,
    us=1000,
    ms=1000000,
    s=1000000000,
    m=60000000000,
    h=3600000000000,
    d=86400000000000,
    w=86400000000000 * 7,
    l=3652425 * 86400 * 100000 // 12,
    q=3652425 * 86400 * 100000 // 4,
    y=3652425 * 86400 * 100000,
)

_parse_interval_timestruct_adjustment = dict(
    d=(2, 1),
    w=(2, 7),
    l=(1, 1),
    q=(1, 3),
    y=(0, 1),
)


def parse_interval(s, future=None, relative_to=None):
    """Parse a time interval expressed as any number of (nano)seconds, days,
    months or years.

    The default is to interpret days and months as simple numbers of
    nanoseconds based on an average calendar year.

    However, if the `future` parameter is not `None`, the algorithm will take
    the specific length of days and months into account. Intervals can then be
    interpreted as being between a certain point in history and now, or between
    now and a certain point in the future. Calculations are done in the local
    time zone.

    The interval string is built up by concatenating integer numbers with their
    units. For example, ``1d2h`` denotes one day and two hours. Supported units
    are:

    == =======
    ns Nanoseconds.
    us Microseconds.
    ms Milliseconds.
    s  Seconds.
    m  Minutes.
    h  Hours.
    d  Days
    w  Weeks.
    l  Months (mnemonic: lunar).
    q  Quarters (a division of a year in four parts of three months each).
    y  Years.
    == =======

    Fractional and non-decimal numbers are currently not supported.
    Whitespace may be used at any place as long as it doesn't split up
    numbers or units.

    Example::

            now = 1581939296000000000 # 2020-02-17 12:34:56 CET
            then = now + parse_interval('2l', True, now)
            then # 1587119696000000000 (2020-04-17 12:34:56 CEST)

    Note how the day of the month and the time stayed the same, even though
    there was a leap day and DST switch inside this interval.

    :param str s: The interval in the syntax described above.
    :param future: If `None`: the interval is based on an average
            calendar year; if `True`: the interval is between now and a certain point
            in the future; if `False`: the interval is between a certain point in
            history and now.
    :type future: bool or None
    :param relative_to: The starting/ending point of the interval is not now but
            the specified moment, expressed as nanoseconds since 1970-01-01 00:00:00
            UTC.
    :return: The interval as a scalar number of nanoseconds.
    :rtype: int"""

    match = _parse_interval_re.match(s)
    if match is None:
        raise Exception("unable to parse date expression '%s'" % (s,))

    result = 0
    then = None

    localtime = _localtime
    mktime = _mktime

    while match is not None:
        number = int(match.group(1))
        unit = match.group(2).lower()

        if future is None:
            result += number * _parse_interval_units[unit]
        else:
            adjustment = _parse_interval_timestruct_adjustment.get(unit)
            if adjustment is not None:
                if then is None:
                    if relative_to is None:
                        relative_to = time_ns()
                    relative_to_seconds = relative_to // 1000000000
                    relative_to_local = localtime(relative_to_seconds)
                    then = list(relative_to_local)
                    then[8] = -1  # is_dst
                what, howmuch = adjustment
                if future:
                    then[what] += howmuch * number
                else:
                    then[what] -= howmuch * number
            else:
                result += number * _parse_interval_units[unit]

        match = _parse_interval_re.match(s, match.end())

    if then is not None:
        then_seconds = int(mktime(tuple(then)))
        if future:
            then_interval = then_seconds - relative_to_seconds
        else:
            then_interval = relative_to_seconds - then_seconds
        result += then_interval * 1000000000

    return result


def _day_interval(a, b, number, offset):
    a_struct = _localtime(a // 1000000000)
    a_day = (
        int(
            _timegm(
                (
                    a_struct.tm_year,
                    a_struct.tm_mon,
                    a_struct.tm_mday,
                    0,
                    0,
                    0,
                    0,
                    0,
                    -1,
                )
            )
            // 86400
        )
        - offset
    )
    a_day, a_day_remainder = divmod(a_day, number)
    a_day_start = int(
        _mktime(
            (
                a_struct.tm_year,
                a_struct.tm_mon,
                a_struct.tm_mday - a_day_remainder,
                0,
                0,
                0,
                0,
                0,
                -1,
            )
        )
        * 1000000000
    )
    a_day_end = int(
        _mktime(
            (
                a_struct.tm_year,
                a_struct.tm_mon,
                a_struct.tm_mday - a_day_remainder + number,
                0,
                0,
                0,
                0,
                0,
                -1,
            )
        )
        * 1000000000
    )
    a_day_ratio = _Fraction(a - a_day_start, a_day_end - a_day_start)

    b_struct = _localtime(b // 1000000000)
    b_day = (
        int(
            _timegm(
                (
                    b_struct.tm_year,
                    b_struct.tm_mon,
                    b_struct.tm_mday,
                    0,
                    0,
                    0,
                    0,
                    0,
                    -1,
                )
            )
            // 86400
        )
        - offset
    )
    b_day, b_day_remainder = divmod(b_day, number)
    if a_day == b_day:
        b_day_start = a_day_start
        b_day_end = a_day_end
    else:
        b_day_start = int(
            _mktime(
                (
                    b_struct.tm_year,
                    b_struct.tm_mon,
                    b_struct.tm_mday - b_day_remainder,
                    0,
                    0,
                    0,
                    0,
                    0,
                    -1,
                )
            )
            * 1000000000
        )
        b_day_end = int(
            _mktime(
                (
                    b_struct.tm_year,
                    b_struct.tm_mon,
                    b_struct.tm_mday - b_day_remainder + number,
                    0,
                    0,
                    0,
                    0,
                    0,
                    -1,
                )
            )
            * 1000000000
        )
    b_day_ratio = _Fraction(b - b_day_start, b_day_end - b_day_start)

    return _Fraction(b_day - a_day, 1) + b_day_ratio - a_day_ratio


def day_interval(a, b):
    """The number of days between two points in time, each expressed as
    nanoseconds since 1970-01-01 00:00:00 UTC. Calculations are done in
    local time.

    Results are undefined if `a` > `b`.

    :param int a: The start of the interval.
    :param int b: The end of the interval.
    :return: The number of days between the intervals.
    :rtype: fractions.Fraction"""

    return _day_interval(a, b, 1, 0)


def week_interval(a, b):
    """The number of weeks between two points in time, each expressed as
    nanoseconds since 1970-01-01 00:00:00 UTC. Calculations are done in
    local time.

    Results are undefined if `a` > `b`.

    :param int a: The start of the interval.
    :param int b: The end of the interval.
    :return: The number of weeks between the intervals.
    :rtype: fractions.Fraction"""

    return _day_interval(a, b, 7, 5)


def _month_interval(a, b, number):
    a_struct = _localtime(a // 1000000000)
    a_yearmonth = a_struct.tm_year * 12 + a_struct.tm_mon
    a_yearmonth, a_yearmonth_remainder = divmod(a_yearmonth, number)
    a_month_start = int(
        _mktime(
            (
                a_struct.tm_year,
                a_struct.tm_mon - a_yearmonth_remainder,
                1,
                0,
                0,
                0,
                0,
                0,
                -1,
            )
        )
        * 1000000000
    )
    a_month_end = int(
        _mktime(
            (
                a_struct.tm_year,
                a_struct.tm_mon - a_yearmonth_remainder + number,
                1,
                0,
                0,
                0,
                0,
                0,
                -1,
            )
        )
        * 1000000000
    )
    a_month_ratio = _Fraction(a - a_month_start, a_month_end - a_month_start)

    b_struct = _localtime(b // 1000000000)
    b_yearmonth = b_struct.tm_year * 12 + b_struct.tm_mon
    b_yearmonth, b_yearmonth_remainder = divmod(b_yearmonth, number)
    if a_yearmonth == b_yearmonth:
        b_month_start = a_month_start
        b_month_end = a_month_end
    else:
        b_month_start = int(
            _mktime(
                (
                    b_struct.tm_year,
                    b_struct.tm_mon - b_yearmonth_remainder,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    -1,
                )
            )
            * 1000000000
        )
        b_month_end = int(
            _mktime(
                (
                    b_struct.tm_year,
                    b_struct.tm_mon - b_yearmonth_remainder + number,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    -1,
                )
            )
            * 1000000000
        )
    b_month_ratio = _Fraction(b - b_month_start, b_month_end - b_month_start)

    return _Fraction(b_yearmonth - a_yearmonth, 1) + b_month_ratio - a_month_ratio


def month_interval(a, b):
    """The number of months between two points in time, each expressed as
    nanoseconds since 1970-01-01 00:00:00 UTC. Calculations are done in
    local time.

    Results are undefined if `a` > `b`.

    :param int a: The start of the interval.
    :param int b: The end of the interval.
    :return: The number of months between the intervals.
    :rtype: fractions.Fraction"""

    return _month_interval(a, b, 1)


def quarter_interval(a, b):
    """The number of quarters (a division of a year in four parts of three
    months each) between two points in time, each expressed as nanoseconds
    since 1970-01-01 00:00:00 UTC. Calculations are done in local time.

    Results are undefined if `a` > `b`.

    :param int a: The start of the interval.
    :param int b: The end of the interval.
    :return: The number of quarters between the intervals.
    :rtype: fractions.Fraction"""

    return _month_interval(a, b, 3)


def year_interval(a, b):
    """The number of years between two points in time, each expressed as
    nanoseconds since 1970-01-01 00:00:00 UTC. Calculations are done in
    local time.

    Results are undefined if `a` > `b`.

    :param int a: The start of the interval.
    :param int b: The end of the interval.
    :return: The number of years between the intervals.
    :rtype: fractions.Fraction"""

    a_struct = _localtime(a // 1000000000)
    a_year = a_struct.tm_year
    a_year_start = int(
        _mktime(
            (
                a_struct.tm_year,
                1,
                1,
                0,
                0,
                0,
                0,
                0,
                -1,
            )
        )
        * 1000000000
    )
    a_year_end = int(
        _mktime(
            (
                a_struct.tm_year + 1,
                1,
                1,
                0,
                0,
                0,
                0,
                0,
                -1,
            )
        )
        * 1000000000
    )
    a_year_ratio = _Fraction(a - a_year_start, a_year_end - a_year_start)

    b_struct = _localtime(b // 1000000000)
    b_year = b_struct.tm_year
    if a_year == b_year:
        b_year_start = a_year_start
        b_year_end = a_year_end
    else:
        b_year_start = int(
            _mktime(
                (
                    b_struct.tm_year,
                    1,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    -1,
                )
            )
            * 1000000000
        )
        b_year_end = int(
            _mktime(
                (
                    b_struct.tm_year + 1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    -1,
                )
            )
            * 1000000000
        )
    b_year_ratio = _Fraction(b - b_year_start, b_year_end - b_year_start)

    return _Fraction(b_year - a_year, 1) + b_year_ratio - a_year_ratio

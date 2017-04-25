from __future__ import absolute_import, division, print_function

import functools
import operator
import sys


PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2


if PY3:
    import builtins
    from itertools import zip_longest
    from io import StringIO, BytesIO
    unicode = str
    long = int
    zip = zip
    range = range
    reduce = functools.reduce
    operator_div = operator.truediv
else:
    import __builtin__ as builtins
    from itertools import izip_longest as zip_longest, izip as zip
    from StringIO import StringIO
    from io import BytesIO
    unicode = unicode
    long = long
    apply = apply
    range = xrange
    reduce = reduce
    operator_div = operator.div

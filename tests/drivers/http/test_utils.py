# coding: utf-8

from clickhouse_sqlalchemy.drivers.http.utils import unescape, parse_tsv
from clickhouse_sqlalchemy.util import compat
from tests.testcase import BaseTestCase


class HttpUtilsTestCase(BaseTestCase):
    def test_unescape(self):
        test_values = [b'', b'a', b'\xff']
        actual = [unescape(t) for t in test_values]
        self.assertListEqual(actual, [u'', u'a', u'\ufffd'])

    def test_unescape_surrogates(self):
        if compat.PY2:
            self.skipTest('surrogateescape is not supported in Python 2')

        test_values = [b'', b'a', b'\xc3\xa3\xff\xf6\0\x00\n', b'a\n\\0']
        actual = [unescape(t, errors='surrogateescape') for t in test_values]
        expected = [u'', u'a', u'ã\udcff\udcf6\x00\x00\n', u'a\n\x00']
        self.assertListEqual(actual, expected)

    def test_reverse_surrogates(self):
        if compat.PY2:
            self.skipTest('surrogateescape is not supported in Python 2')

        # What's stored in the database:
        expected = [b'', b'a', b'\xc3\xa3\xff\x55\xf6\0\x00\x45\0\n']

        # What comes over the wire:
        test_values = [b'', b'a', b'\xc3\xa3\xff\x55\xf6\0\x00\x45\\0\n']

        escaped = [unescape(t, errors='surrogateescape') for t in test_values]
        actual = [t.encode('utf-8', errors='surrogateescape') for t in escaped]
        self.assertListEqual(actual, expected)

    def test_parse_tsv(self):
        test_values = [b'', b'a\tb\tc\n', b'a\tb\t\xff']
        expected = [[u''], [u'a', u'b', u'c\n'], [u'a', u'b', u'\ufffd']]
        try:
            actual = [parse_tsv(value) for value in test_values]
        except IndexError:
            self.fail('"parse_tsv" raised IndexError exception!')
        except TypeError:
            self.fail('"parse_tsv" raised TypeError exception!')

        self.assertListEqual(actual, expected)

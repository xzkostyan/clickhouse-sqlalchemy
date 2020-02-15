from clickhouse_sqlalchemy.drivers.http.utils import unescape, parse_tsv
from tests.testcase import BaseTestCase


class HttpUtilsTestCase(BaseTestCase):
    def test_unescape(self):
        test_values = [b'', b'a', b'\xff']
        actual = [unescape(t) for t in test_values]
        self.assertListEqual(actual, [u'', u'a', u'\ufffd'])

    def test_parse_tsv(self):
        test_values = [b'', b'a\tb\tc', b'a\tb\t\xff']
        try:
            for value in test_values:
                parse_tsv(value)
        except IndexError:
            self.fail('"parse_tsv" raised IndexError exception!')
        except TypeError:
            self.fail('"parse_tsv" raised TypeError exception!')

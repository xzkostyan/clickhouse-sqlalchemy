from clickhouse_sqlalchemy.drivers.http.utils import unescape, parse_tsv
from tests.testcase import BaseTestCase


class HttpUtilsTestCase(BaseTestCase):
    def test_unescape(self):
        test_values = [b'', b'a', b'\xff']
        actual = [unescape(t) for t in test_values]
        assert actual == ['', 'a', '�']        

    def test_parse_tsv(self):
        test_values = [b'', b'a\tb\tc', b'a\tb\t\xff']
        try:
            for v in test_values:
                parse_tsv(v)
        except IndexError:
            self.fail('"parse_tsv" raised IndexError exception!')
        except TypeError:
            self.fail('"parse_tsv" raised TypeError exception!')

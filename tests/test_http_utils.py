from src.drivers.http.utils import parse_tsv
from tests.testcase import BaseTestCase


class HttpUtilsTestCase(BaseTestCase):
    test_values = [b'', b'a\tb\tc']

    def test_parse_tsv(self):
        try:
            for v in self.test_values:
                parse_tsv(v)
        except IndexError:
            self.fail('"parse_tsv" raised IndexError exception!')
        except TypeError:
            self.fail('"parse_tsv" raised TypeError exception!')

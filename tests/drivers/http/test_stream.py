
from tests.testcase import HttpSessionTestCase
from tests.session import http_stream_session


class StreamingHttpTestCase(HttpSessionTestCase):
    session = http_stream_session

    def test_streaming(self):
        power = 4
        # `10**power` rows.
        # Timing order of magnitude:
        # est. 1 second on 10**5 lines for TSV,
        # 2.5 seconds on uncythonized native protocol.
        query = 'select ' + ', '.join(
            ("arrayJoin(["
             "'{0}', '2', '3', '4', '5', "
             "'6', '7', '8', '9', '10']) as a{0}").format(num)
            for num in range(power))
        res = self.session.execute(query)
        count = sum(1 for _ in res)
        assert count == 10 ** power

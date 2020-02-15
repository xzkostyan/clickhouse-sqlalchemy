
from tests.testcase import HttpSessionTestCase
from tests.session import http_stream_session


class StreamingHttpTestCase(HttpSessionTestCase):

    session = http_stream_session
    power = 4

    def make_query(self, power=None):
        if power is None:
            power = self.power
        # `10**power` rows.
        # Timing order of magnitude:
        # est. 1 second on 10**5 lines for TSV,
        # 2.5 seconds on uncythonized native protocol.
        query = 'select ' + ', '.join(
            ("arrayJoin(["
             "'{0}', '2', '3', '4', '5', "
             "'6', '7', '8', '9', '10']) as a{0}").format(num)
            for num in range(power))
        return query

    def test_streaming(self):
        power = self.power
        query = self.make_query(power=power)
        res = self.session.execute(query)
        count = sum(1 for _ in res)
        self.assertEqual(count, 10 ** power)

    def test_fetchmany(self):
        power = self.power - 1
        query = self.make_query(power=power)
        res = self.session.execute(query)

        count = 0
        while True:
            block = res.fetchmany(1000)
            if not block:
                break
            count += sum(1 for _ in block)

        self.assertEqual(count, 10 ** power)

    def test_fetchall(self):
        power = self.power - 2
        if power < 1:
            raise Exception(
                "Misconfigured test case:"
                " `power` should be at least 3.")
        query = self.make_query(power=power)
        res = self.session.execute(query)

        count = 0

        block = res.fetchmany(7)
        count += sum(1 for _ in block)

        block = res.fetchall()
        count += sum(1 for _ in block)

        self.assertEqual(count, 10 ** power)

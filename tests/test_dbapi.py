from unittest import TestCase
import pandas as pd
from calhipan import dbapi
from . import eq_


class DBAPITest(TestCase):
    def _simple_fixture(self):
        df1 = pd.DataFrame([
                {
                    'col1': i,
                    'col2': "string: %d" % i,
                    'col3': i * .005473
                } for i in xrange(5)
            ])
        df2 = pd.DataFrame([
                {"name": "ed", "fullname": "Ed Jones"},
                {"name": "jack", "fullname": "Jack"},
                {"name": "wendy", "fullname": "Wendy"},
                ])
        return {"df1": df1, "df2": df2}

    def test_simple_statement(self):
        def stmt(namespace, params):
            return namespace["df1"][["col1", "col2", "col3"]]

        conn = dbapi.connect(self._simple_fixture())
        curs = conn.cursor()
        curs.execute(stmt)
        eq_(
            curs.fetchall(),
            [(0, 'string: 0', 0.0), (1, 'string: 1', 0.005473),
            (2, 'string: 2', 0.010946),
            (3, 'string: 3', 0.016419), (4, 'string: 4', 0.021892)]
        )

    def test_description(self):
        def stmt(namespace, params):
            return namespace["df1"][["col1", "col2", "col3"]]

        conn = dbapi.connect(self._simple_fixture())
        curs = conn.cursor()
        curs.execute(stmt)
        eq_(
            curs.description, [
                ('col1', None, None, None, None, None, None),
                ('col2', None, None, None, None, None, None),
                ('col3', None, None, None, None, None, None)
            ]
        )


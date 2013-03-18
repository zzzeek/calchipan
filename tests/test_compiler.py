from unittest import TestCase
import pandas as pd
from calhipan import dbapi, base
from . import eq_
from sqlalchemy import Table, Column, Integer, Float, String, MetaData, select

class RoundTripTest(TestCase):
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

    def _table_fixture(self):
        m = MetaData()
        Table('df1', m,
                        Column('col1', Integer),
                        Column('col2', String),
                        Column('col3', Float))

        Table('df2', m,
                        Column('name', String),
                        Column('fullname', String))
        return m.tables


    def _exec_stmt(self, stmt):
        conn = dbapi.connect(self._simple_fixture())

        d = base.PandasDialect()
        comp = stmt.compile(dialect=d)
        curs = conn.cursor()
        curs.execute(comp._panda_fn, comp.params)
        return curs

    def test_select_single_table(self):
        t1 = self._table_fixture()['df1']
        stmt = t1.select()
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [(0, 'string: 0', 0.0),
            (1, 'string: 1', 0.005473),
            (2, 'string: 2', 0.010946),
            (3, 'string: 3', 0.016419),
            (4, 'string: 4', 0.021892)]
        )

    def test_select_column_expression(self):
        t1 = self._table_fixture()['df1']
        stmt = select([t1.c.col1 + 10])
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [(10,), (11,), (12,), (13,), (14,)]
        )
    def test_select_single_table_whereclause(self):
        t1 = self._table_fixture()['df1']
        stmt = t1.select().where(t1.c.col3 > .012)
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [(3, 'string: 3', 0.016419),
            (4, 'string: 4', 0.021892)]
        )

    def test_select_alias(self):
        t1 = self._table_fixture()['df1']
        t1a = t1.alias()
        stmt = t1a.select().where(t1a.c.col3 > .012)
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [(3, 'string: 3', 0.016419),
            (4, 'string: 4', 0.021892)]
        )

    def test_selfref_join(self):
        t2 = self._table_fixture()['df2']
        t2a = t2.alias()
        t2b = t2.alias()

        stmt = select([t2a.c.name, t2b.c.name]).where(t2a.c.name < t2b.c.name)
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [('ed', 'jack'), ('ed', 'wendy'), ('jack', 'wendy')]
        )

        eq_(
            curs.description,
            []
        )
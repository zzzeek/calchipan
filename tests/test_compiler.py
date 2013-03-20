from unittest import TestCase
import pandas as pd
from calhipan import dbapi, base
from . import eq_
from sqlalchemy import Table, Column, Integer, Float, \
        String, MetaData, select, and_, or_

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
        df3 = pd.DataFrame([
                {"name": "ed", "data": "ed1"},
                {"name": "ed", "data": "ed2"},
                {"name": "ed", "data": "ed3"},
                {"name": "jack", "data": "jack1"},
                {"name": "jack", "data": "jack2"},
                ])

        return {"df1": df1, "df2": df2, "df3": df3}

    def _table_fixture(self):
        m = MetaData()
        Table('df1', m,
                        Column('col1', Integer),
                        Column('col2', String),
                        Column('col3', Float))

        Table('df2', m,
                        Column('name', String),
                        Column('fullname', String))

        Table('df3', m,
                        Column('name', String),
                        Column('data', String))

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

    def test_labeling(self):
        t1 = self._table_fixture()['df1']
        stmt = select([t1.c.col1.label('q'), t1.c.col2.label('p')])
        curs = self._exec_stmt(stmt)
        eq_([c[0] for c in curs.description], ['q', 'p'])

        eq_(curs.fetchall(),
                [(0, 'string: 0'), (1, 'string: 1'), (2, 'string: 2'),
                (3, 'string: 3'), (4, 'string: 4')])

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
            [c[0] for c in curs.description],
            ['name', 'name_1']
        )

    def test_implicit_join_where(self):
        tables = self._table_fixture()
        df2, df3 = tables['df2'], tables['df3']

        stmt = select([df2.c.name, df3.c.data]).\
                    where(df2.c.name == df3.c.name).\
                    where(df2.c.name == 'ed')
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [('ed', 'ed1'), ('ed', 'ed2'), ('ed', 'ed3')]
        )

    def test_explicit_simple_join(self):
        tables = self._table_fixture()
        df2, df3 = tables['df2'], tables['df3']

        stmt = select([df2.c.name, df3.c.data]).\
                    select_from(df2.join(df3, df2.c.name == df3.c.name)).\
                    where(df2.c.name == 'ed')
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [('ed', 'ed1'), ('ed', 'ed2'), ('ed', 'ed3')]
        )

    def test_explicit_simple_join_reverse_onclause(self):
        tables = self._table_fixture()
        df2, df3 = tables['df2'], tables['df3']

        stmt = select([df2.c.name, df3.c.data]).\
                    select_from(df2.join(df3, df3.c.name == df2.c.name)).\
                    where(df2.c.name == 'ed')
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [('ed', 'ed1'), ('ed', 'ed2'), ('ed', 'ed3')]
        )

    def test_explicit_simple_join_aliased(self):
        tables = self._table_fixture()
        df2, df3 = tables['df2'], tables['df3']

        s1 = select([(df2.c.name + "hi").label('name')]).alias()
        s2 = select([(df3.c.name + "hi").label('name'), df3.c.data]).\
                where(or_(
                            df3.c.data == 'ed2',
                            df3.c.data == 'jack1',
                            df3.c.data == 'jack2'
                        )).\
                alias()

        stmt = select([s1.c.name, s2.c.name, s2.c.data]).\
            select_from(s1.join(s2, s1.c.name == s2.c.name))


        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [('edhi', 'edhi', 'ed2'), ('jackhi', 'jackhi', 'jack1'),
                ('jackhi', 'jackhi', 'jack2')]
        )

    def test_explicit_compound_join(self):
        tables = self._table_fixture()
        df2, df3 = tables['df2'], tables['df3']

        stmt = select([df2.c.name, df3.c.data]).\
                    select_from(df2.join(df3,
                            and_(df2.c.name == df3.c.name,
                                    df2.c.name == df3.c.name)
                        )).\
                    where(df2.c.name == 'ed')
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [('ed', 'ed1'), ('ed', 'ed2'), ('ed', 'ed3')]
        )

    def test_explicit_complex_join(self):
        tables = self._table_fixture()
        df2, df3 = tables['df2'], tables['df3']

        stmt = select([df2.c.name, df3.c.data]).\
                    select_from(df2.join(df3,
                            and_(df2.c.name == df3.c.name,
                                    df2.c.name == 'ed'))
                        )
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [('ed', 'ed1'), ('ed', 'ed2'), ('ed', 'ed3')]
        )

    def test_correlated_subquery_column(self):
        tables = self._table_fixture()
        df2, df3 = tables['df2'], tables['df3']

        subq = select([df2.c.name]).where(df2.c.name == df3.c.name).as_scalar()
        stmt = select([
                    df3.c.data,
                    subq
                ])
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [('ed1', 'ed'), ('ed2', 'ed'), ('ed3', 'ed'),
                ('jack1', 'jack'), ('jack2', 'jack')]
        )

    def test_correlated_subquery_column_wnull(self):
        tables = self._table_fixture()
        df2, df3 = tables['df2'], tables['df3']

        subq = select([df3.c.name]).\
                    where(df2.c.name == df3.c.name).\
                    where(or_(df3.c.data == "jack2", df3.c.data == 'ed1')).\
                    as_scalar()
        stmt = select([df2, subq])
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [('ed', 'Ed Jones', 'ed'), ('jack', 'Jack', 'jack'),
                ('wendy', 'Wendy', None)]
        )

    def test_correlated_subquery_whereclause(self):
        tables = self._table_fixture()
        df2, df3 = tables['df2'], tables['df3']

        subq = select([df3.c.name]).\
                    where(df2.c.name == df3.c.name).\
                    where(or_(df3.c.data == "jack2", df3.c.data == 'ed1')).\
                    as_scalar()
        stmt = select([df2]).where(df2.c.name == subq)
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [('ed', 'Ed Jones'), ('jack', 'Jack')]
        )


    def test_uncorrelated_subquery_whereclause(self):
        tables = self._table_fixture()
        df2, df3 = tables['df2'], tables['df3']

        subq = select([df2.c.name]).\
                    where(df2.c.fullname == 'Jack').\
                    as_scalar()
        stmt = select([df3]).where(df3.c.name == subq)
        curs = self._exec_stmt(stmt)
        eq_(
            curs.fetchall(),
            [('jack', 'jack1'), ('jack', 'jack2')]
        )


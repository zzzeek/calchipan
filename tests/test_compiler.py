from unittest import TestCase
import pandas as pd
from calhipan import dbapi, base
from . import eq_, assert_raises_message
from sqlalchemy import Table, Column, Integer, union_all, \
        String, MetaData, select, and_, or_, ForeignKey

class RoundTripTest(TestCase):

    def _emp_d_fixture(self):
        emp_df = pd.DataFrame([
                {"emp_id": 1, "name": "ed", "fullname": "Ed Jones",
                        "dep_id": 1},
                {"emp_id": 2, "name": "wendy", "fullname": "Wendy Wharton",
                        "dep_id": 1},
                {"emp_id": 3, "name": "jack", "fullname": "Jack Smith",
                        "dep_id": 2},

            ])
        dept_df = pd.DataFrame([
                    {"dep_id": 1, "name": "Engineering"},
                    {"dep_id": 2, "name": "Accounting"},
                    {"dep_id": 3, "name": "Sales"},
                    ])
        m = MetaData()
        emp = Table('employee', m,
                    Column('emp_id', Integer, primary_key=True),
                    Column('name', String),
                    Column('fullname', String),
                    Column('dep_id', Integer, ForeignKey('department.dep_id'))
            )
        dep = Table('department', m,
                    Column('dep_id', Integer, primary_key=True),
                    Column('name', String),
                    )
        conn = dbapi.connect({"employee": emp_df, "department": dept_df},
                        dbapi.Trace(log=True))
        return emp, dep, conn

    def test_select_single_table(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([dep]).where(dep.c.dep_id > 1)
                )
        eq_(
            r.fetchall(),
            [(2, 'Accounting'), (3, 'Sales')]
        )


    def test_select_single_alias(self):
        emp, dep, conn = self._emp_d_fixture()
        da = dep.alias()
        r = self._exec_stmt(conn,
                    select([da]).where(da.c.dep_id > 1)
                )
        eq_(
            r.fetchall(),
            [(2, 'Accounting'), (3, 'Sales')]
        )

    def test_select_implicit_join(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([emp, dep]).\
                        where(emp.c.dep_id == dep.c.dep_id)
                    )
        eq_(r.fetchall(),
            [
            (1, 'ed', 'Ed Jones', 1, 1, 'Engineering'),
            (2, 'wendy', 'Wendy Wharton', 1, 1, 'Engineering'),
            (3, 'jack', 'Jack Smith', 2, 2, 'Accounting')
        ])
        self._assert_cartesian(conn.trace)

    def test_select_column_expression(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([
                        "name: " + emp.c.name,
                        emp.c.dep_id + 8,
                        emp.c.dep_id
                    ]).\
                        where(emp.c.dep_id - 1 == 0)
                )
        eq_(r.fetchall(),
            [('name: ed', 9, 1), ('name: wendy', 9, 1)]
        )

    def test_select_explicit_join_simple_crit(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([emp, dep]).select_from(
                        emp.join(dep, emp.c.dep_id == dep.c.dep_id)
                    )
                )
        eq_(r.fetchall(),
            [
            (1, 'ed', 'Ed Jones', 1, 1, 'Engineering'),
            (2, 'wendy', 'Wendy Wharton', 1, 1, 'Engineering'),
            (3, 'jack', 'Jack Smith', 2, 2, 'Accounting')
        ])
        self._assert_no_cartesian(conn.trace)

    def test_select_explicit_join_simple_reverse_crit(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([emp, dep]).select_from(
                        emp.join(dep, dep.c.dep_id == emp.c.dep_id))
                )
        eq_(r.fetchall(),
            [
            (1, 'ed', 'Ed Jones', 1, 1, 'Engineering'),
            (2, 'wendy', 'Wendy Wharton', 1, 1, 'Engineering'),
            (3, 'jack', 'Jack Smith', 2, 2, 'Accounting')
        ])
        # no cartesian product
        self._assert_no_cartesian(conn.trace)

    def test_explicit_simple_join_aliased(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([dep.c.dep_id, ("name: " + dep.c.name).label('name')]).\
                    alias()
        s2 = select([emp.c.dep_id, ("name: " + emp.c.name).label('name')]).\
                where(
                    or_(
                        emp.c.name == 'jack',
                        emp.c.name == 'wendy'
                    )
                ).alias()
        stmt = select([s1, s2]).select_from(
                    s1.join(s2, s1.c.dep_id == s2.c.dep_id))
        r = self._exec_stmt(conn, stmt)
        eq_(r.fetchall(),
            [(1, 'name: Engineering', 1, 'name: wendy'),
            (2, 'name: Accounting', 2, 'name: jack')]
        )
        self._assert_no_cartesian(conn.trace)

    def test_select_explicit_join_complex_crit(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([emp, dep]).\
                        select_from(emp.join(dep, emp.c.dep_id < dep.c.dep_id))
                )
        eq_(r.fetchall(), [
            (1, 'ed', 'Ed Jones', 1, 2, 'Accounting'),
            (1, 'ed', 'Ed Jones', 1, 3, 'Sales'),
            (2, 'wendy', 'Wendy Wharton', 1, 2, 'Accounting'),
            (2, 'wendy', 'Wendy Wharton', 1, 3, 'Sales'),
            (3, 'jack', 'Jack Smith', 2, 3, 'Sales')
        ])
        self._assert_cartesian(conn.trace)

    def test_select_explicit_join_compound_crit(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([emp, dep]).\
                        select_from(emp.join(dep,
                            and_(emp.c.dep_id == dep.c.dep_id,
                                emp.c.name > dep.c.name)))
                )
        eq_(r.fetchall(), [
            (1, 'ed', 'Ed Jones', 1, 1, 'Engineering'),
            (2, 'wendy', 'Wendy Wharton', 1, 1, 'Engineering'),
            (3, 'jack', 'Jack Smith', 2, 2, 'Accounting')
        ])
        self._assert_no_cartesian(conn.trace)


    def test_correlated_subquery_column(self):
        emp, dep, conn = self._emp_d_fixture()

        subq = select([dep.c.name]).\
                    where(dep.c.dep_id == emp.c.dep_id).as_scalar()
        stmt = select([emp.c.name, subq])
        r = self._exec_stmt(conn, stmt)
        eq_(r.fetchall(),
            [('ed', 'Engineering'), ('wendy', 'Engineering'),
            ('jack', 'Accounting')])


    def test_correlated_subquery_column_null(self):
        emp, dep, conn = self._emp_d_fixture()

        subq = select([emp.c.name]).\
                    where(dep.c.dep_id == emp.c.dep_id).\
                    where(or_(emp.c.name == 'jack', emp.c.name == 'wendy')).\
                    as_scalar()
        stmt = select([dep.c.name, subq])
        r = self._exec_stmt(conn, stmt)
        eq_(r.fetchall(),
            [('Engineering', 'wendy'), ('Accounting', 'jack'),
            ('Sales', None)])


    def test_multirow_subquery_error(self):
        emp, dep, conn = self._emp_d_fixture()

        subq = select([emp.c.name]).\
                    where(dep.c.dep_id == emp.c.dep_id).as_scalar()
        stmt = select([dep.c.name, subq])
        assert_raises_message(
            Exception,
            "Subquery returned more than one row",
            self._exec_stmt, conn, stmt,
        )

    def test_correlated_subquery_whereclause(self):
        emp, dep, conn = self._emp_d_fixture()

        subq = select([dep.c.dep_id]).\
                    where(dep.c.dep_id == emp.c.dep_id).\
                    where(or_(emp.c.name == 'jack', emp.c.name == 'wendy')).\
                    as_scalar()
        stmt = select([emp.c.name, subq]).where(emp.c.dep_id == subq)
        r = self._exec_stmt(conn, stmt)
        eq_(r.fetchall(), [('wendy', 1), ('jack', 2)])

    def test_uncorrelated_subquery_whereclause(self):
        emp, dep, conn = self._emp_d_fixture()

        subq = select([dep.c.dep_id]).\
                    where(dep.c.name == "Engineering").\
                    as_scalar()
        stmt = select([emp.c.name, subq]).where(emp.c.dep_id == subq)
        r = self._exec_stmt(conn, stmt)
        eq_(r.fetchall(), [('ed', 1), ('wendy', 1)])


    def _assert_cartesian(self, trace):
        assert self._has_cartesian(trace)

    def _assert_no_cartesian(self, trace):
        assert not self._has_cartesian(trace)

    def _has_cartesian(self, trace):
        for elem in trace._buf:
            if elem[0] == "merge" and \
                    len(elem[3]) == len(elem[1]) * len(elem[2]):
                return True
        else:
            return False

    def test_labeling(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([
                    emp.c.name.label('emp name'),
                    (emp.c.dep_id + 5).label('dep plus 5')
                ])
        r = self._exec_stmt(conn, stmt)
        eq_(r.fetchall(), [
            ('ed', 6), ('wendy', 6), ('jack', 7)
        ])
        eq_([c[0] for c in r.description], ['emp name', 'dep plus 5'])


    def test_selfref_join(self):
        emp, dep, conn = self._emp_d_fixture()

        emp_a1 = emp.alias()
        emp_a2 = emp.alias()

        stmt = select([emp_a1.c.name, emp_a2.c.name]).\
                        where(emp_a1.c.name < emp_a2.c.name)
        r = self._exec_stmt(conn, stmt)
        eq_(r.fetchall(), [
            ('ed', 'wendy'), ('ed', 'jack'), ('jack', 'wendy')
        ])
        eq_(
            [c[0] for c in r.description],
            ['name', 'name_1']
        )

    def test_union_homogeneous(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([emp.c.name]).where(emp.c.name == "wendy")
        s2 = select([emp.c.name]).where(or_(emp.c.name == 'wendy',
                        emp.c.name == 'jack'))
        u1 = union_all(s1, s2)
        r = self._exec_stmt(conn, u1)
        eq_(r.fetchall(),
            [('wendy',), ('wendy',), ('jack',)])

    def test_union_heterogeneous_columns(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([emp.c.name])
        s2 = select([dep.c.name])
        u1 = union_all(s1, s2)
        r = self._exec_stmt(conn, u1)
        eq_(r.fetchall(),
            [('ed',), ('wendy',), ('jack',),
                ('Engineering',), ('Accounting',), ('Sales',)])

    def test_union_heterogeneous_types(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([emp.c.name, emp.c.fullname]).where(emp.c.name == 'jack')
        s2 = select([dep.c.dep_id, dep.c.name])
        u1 = union_all(s1, s2)
        r = self._exec_stmt(conn, u1)
        eq_(r.fetchall(), [
            ('jack', 'Jack Smith'), (1, 'Engineering'),
            (2, 'Accounting'), (3, 'Sales')])

    def _exec_stmt(self, conn, stmt, trace=None):
        d = base.PandasDialect()
        comp = stmt.compile(dialect=d)
        curs = conn.cursor()
        curs.execute(comp._panda_fn, comp.params, trace=trace)
        return curs


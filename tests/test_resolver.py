from sqlalchemy.testing.fixtures import TestBase
import pandas as pd
import numpy as np
from calchipan import dbapi, base
from . import eq_, assert_raises_message
from calchipan import aggregate_fn, non_aggregate_fn


from sqlalchemy import Table, Column, Integer, union_all, \
        String, MetaData, select, and_, or_, ForeignKey, \
        func, exc, schema, literal, Float, union

class _ExecBase(object):
    def _exec_stmt(self, conn, stmt):
        d = base.PandasDialect()
        comp = stmt.compile(dialect=d)
        curs = conn.cursor()
        curs.execute(comp._panda_fn, comp.params)
        return curs



class RoundTripTest(_ExecBase, TestBase):

    def setUp(self):
        self._merge = _merge = pd.DataFrame.merge
        self.trace = trace = []
        def wrap_merge(self, other, **kw):
            result = _merge(self, other, **kw)
            trace.append((self, other, result))
            return result
        pd.DataFrame.merge = wrap_merge

    def tearDown(self):
        del self.trace
        pd.DataFrame.merge = self._merge

    def _numbers_fixture(self):
        numbers = pd.DataFrame(np.array(((0.01, 0.01, 0.02, 0.04, 0.03),
              (0.00, 0.02, 0.02, 0.03, 0.02),
              (0.01, 0.02, 0.02, 0.03, 0.02),
              (0.01, 0.00, 0.01, 0.05, 0.03))),
                columns=['a', 'b', 'c', 'd', 'e'])

        m = MetaData()
        n_t = Table('numbers', m,
                Column('a', Float), Column('b', Float),
                Column('c', Float), Column('d', Float),
                Column('e', Float))

        conn = dbapi.connect(
                        {"numbers": numbers})
        return n_t, conn

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
        conn = dbapi.connect(
                        {"employee": emp_df, "department": dept_df})
        return emp, dep, conn

    def _autoinc_fixture(self):
        dept_df = pd.DataFrame([
                    {"name": "Engineering"},
                    {"name": "Accounting"},
                    {"name": "Sales"},
                    ])
        m = MetaData()
        dep = Table('department', m,
                    Column('dep_id', Integer, primary_key=True),
                    Column('name', String),
                    pandas_index_pk=True
                    )
        conn = dbapi.connect(
                        {"department": dept_df})
        return dep, conn

    def test_select_single_table(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([dep]).where(dep.c.dep_id > 1)
                )
        eq_(
            r.fetchall(),
            [(2, 'Accounting'), (3, 'Sales')]
        )

    def test_select_compare_none(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([dep.c.name]).select_from(dep.outerjoin(emp)).\
                        where(emp.c.emp_id == None)
                )
        eq_(
            r.fetchall(),
            [('Sales', )]
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

    def test_select_single_alias_pandas_pk(self):
        dep, conn = self._autoinc_fixture()
        da = dep.alias()
        r = self._exec_stmt(conn,
                    select([da]).where(da.c.dep_id > 0)
                )
        eq_(
            r.fetchall(),
            [(1, 'Accounting'), (2, 'Sales')]
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
        self._assert_cartesian(conn)

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
        self._assert_no_cartesian(conn)

    def test_select_explicit_outerjoin_simple_crit(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([dep.c.name, emp.c.name]).select_from(
                        dep.outerjoin(emp, emp.c.dep_id == dep.c.dep_id)
                    )
                )
        eq_(r.fetchall(),
            [
            ('Engineering', 'ed'), ('Engineering', 'wendy'),
            ('Accounting', 'jack'), ('Sales', None)
        ])
        self._assert_no_cartesian(conn)

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
        self._assert_no_cartesian(conn)

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
        self._assert_no_cartesian(conn)

    def test_select_explicit_join_complex_crit_1(self):
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
        self._assert_cartesian(conn)

    def test_select_explicit_join_complex_crit_2(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([dep.c.name, emp.c.name]).select_from(
                        dep.join(emp, emp.c.dep_id >= dep.c.dep_id)
                    )
                )
        eq_(r.fetchall(),
            [
            ('Engineering', 'ed'), ('Engineering', 'wendy'),
            ('Engineering', 'jack'), ('Accounting', 'jack')
        ])
        self._assert_cartesian(conn)

    def test_select_explicit_outerjoin_complex_crit(self):
        emp, dep, conn = self._emp_d_fixture()
        r = self._exec_stmt(conn,
                    select([dep.c.name, emp.c.name]).select_from(
                        dep.outerjoin(emp, emp.c.dep_id >= dep.c.dep_id)
                    )
                )
        eq_(r.fetchall(),
            [
            ('Engineering', 'ed'), ('Engineering', 'wendy'),
            ('Engineering', 'jack'), ('Accounting', 'jack'),
            ('Sales', None)
        ])
        self._assert_cartesian(conn)


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
        self._assert_no_cartesian(conn)

    def test_select_explicit_join_to_select_1(self):
        emp, dep, conn = self._emp_d_fixture()
        deps = select([dep.c.name, dep.c.dep_id]).\
            where(dep.c.dep_id.in_([1, 2])).alias()
        stmt = select([emp.c.name, deps.c.name]).\
                    select_from(emp.outerjoin(deps))
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('ed', 'Engineering'), ('wendy', 'Engineering'), ('jack', 'Accounting')]
        )

    def test_select_explicit_join_to_select_2(self):
        emp, dep, conn = self._emp_d_fixture()
        deps = select([dep.c.name, dep.c.dep_id]).alias()
        stmt = select([emp.c.name, deps.c.name]).\
                    select_from(deps.outerjoin(emp))
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('ed', 'Engineering'), ('wendy', 'Engineering'),
                ('jack', 'Accounting'), (None, 'Sales')]
        )

    def test_select_labels_1(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([emp.c.name, emp.c.fullname]).alias()
        stmt = select([stmt]).apply_labels().order_by(stmt.c.name)
        r = self._exec_stmt(conn, stmt)
        eq_([c[0] for c in r.description], ['anon_1_name', u'anon_1_fullname'])
        eq_(
            r.fetchall(),
            [('ed', 'Ed Jones'), ('jack', 'Jack Smith'),
                ('wendy', 'Wendy Wharton')]
        )

    def test_select_expression_1(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([3])
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('3',)]
        )

    def test_select_expression_2(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([literal(3)])
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(3,)]
        )

    def test_select_expression_3(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([literal(3) + 5])
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(8,)]
        )


    def test_correlated_subquery_column(self):
        emp, dep, conn = self._emp_d_fixture()

        subq = select([dep.c.name]).\
                    where(dep.c.dep_id == emp.c.dep_id).as_scalar()
        stmt = select([emp.c.name, subq])
        r = self._exec_stmt(conn, stmt)
        eq_(r.fetchall(),
            [('ed', 'Engineering'), ('wendy', 'Engineering'),
            ('jack', 'Accounting')])


    def test_correlated_subquery_bind(self):
        emp, dep, conn = self._emp_d_fixture()

        subq = select([literal("Engineering")]).\
                    where(dep.c.dep_id == emp.c.dep_id).as_scalar()
        stmt = select([emp.c.name, subq])
        r = self._exec_stmt(conn, stmt)
        eq_(r.fetchall(),
            [('ed', 'Engineering'), ('wendy', 'Engineering'),
            ('jack', 'Engineering')])

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
            dbapi.Error,
            "scalar expression returned more than one row",
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


    def _assert_cartesian(self, conn):
        assert self._has_cartesian(conn)

    def _assert_no_cartesian(self, conn):
        assert not self._has_cartesian(conn)

    def _has_cartesian(self, conn):
        for df1, df2, product in self.trace:
            if len(product) == len(df1) * len(df2):
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

    def test_union_all_homogeneous(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([emp.c.name]).where(emp.c.name == "wendy")
        s2 = select([emp.c.name]).where(or_(emp.c.name == 'wendy',
                        emp.c.name == 'jack'))
        u1 = union_all(s1, s2)
        r = self._exec_stmt(conn, u1)
        eq_(r.fetchall(),
            [('wendy',), ('wendy',), ('jack',)])

    def test_union_all_heterogeneous_columns(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([emp.c.name])
        s2 = select([dep.c.name])
        u1 = union_all(s1, s2)
        r = self._exec_stmt(conn, u1)
        eq_(r.fetchall(),
            [('ed',), ('wendy',), ('jack',),
                ('Engineering',), ('Accounting',), ('Sales',)])

    def test_union_all_limit_offset(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([emp.c.name])
        s2 = select([dep.c.name])
        u1 = union_all(s1, s2).order_by(emp.c.name).limit(3).offset(2)
        r = self._exec_stmt(conn, u1)
        eq_(r.fetchall(),
            [('Sales',), ('ed',), ('jack',)])

    def test_union_all_heterogeneous_types(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([emp.c.name, emp.c.fullname]).where(emp.c.name == 'jack')
        s2 = select([dep.c.dep_id, dep.c.name])
        u1 = union_all(s1, s2)
        r = self._exec_stmt(conn, u1)
        eq_(r.fetchall(), [
            ('jack', 'Jack Smith'), (1, 'Engineering'),
            (2, 'Accounting'), (3, 'Sales')])

    def test_union_homogeneous(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([emp.c.name]).where(emp.c.name == "wendy")
        s2 = select([emp.c.name]).where(or_(emp.c.name == 'wendy',
                        emp.c.name == 'jack'))
        u1 = union(s1, s2)
        r = self._exec_stmt(conn, u1)
        eq_(r.fetchall(), [('wendy',), ('jack',)])

    def test_union_heterogeneous_columns(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([emp.c.name])
        s2 = select([dep.c.name])
        u1 = union(s1, s2)
        r = self._exec_stmt(conn, u1)
        eq_(r.fetchall(),
            [('ed',), ('wendy',), ('jack',),
                ('Engineering',), ('Accounting',), ('Sales',)])

    def test_union_limit_offset(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([emp.c.name])
        s2 = select([dep.c.name])
        u1 = union(s1, s2).order_by(emp.c.name).limit(3).offset(2)
        r = self._exec_stmt(conn, u1)
        eq_(r.fetchall(),
            [('Sales',), ('ed',), ('jack',)])

    def test_union_heterogeneous_types(self):
        emp, dep, conn = self._emp_d_fixture()

        s1 = select([emp.c.name, emp.c.fullname]).where(emp.c.name == 'jack')
        s2 = select([dep.c.dep_id, dep.c.name])
        u1 = union(s1, s2)
        r = self._exec_stmt(conn, u1)
        eq_(r.fetchall(), [
            ('jack', 'Jack Smith'), (1, 'Engineering'),
            (2, 'Accounting'), (3, 'Sales')])

    def test_count_function(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([func.count(emp.c.name)])
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(3, )]
        )

    def test_custom_function_aggregate(self):
        n_t, conn = self._numbers_fixture()

        @aggregate_fn()
        def stddev(values):
            return values.std()

        stmt = select([func.stddev(n_t.c.c),
                        func.stddev(n_t.c.d).label('d')])
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(0.005000000000000014, 0.0095742710775633677)]
        )

    def test_custom_function_multidimensional_aggregate(self):
        n_t, conn = self._numbers_fixture()

        @aggregate_fn()
        def fancy(x):
            return x[0][0] + x[1][0]

        stmt = select([fancy(n_t.c.c, n_t.c.d)])
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(0.06,)]
        )

    def test_custom_function_non_aggregate(self):
        emp, dep, conn = self._emp_d_fixture()

        @non_aggregate_fn()
        def add_numbers(x, y):
            return x + y

        stmt = select([add_numbers(1, 2)])
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(3,)]
        )

    def test_custom_function_namespace(self):
        n_t, conn = self._numbers_fixture()

        from calchipan import aggregate_fn
        @aggregate_fn(package='numpy')
        def stddev(values):
            return values.std()

        stmt = select([func.numpy.stddev(n_t.c.c),
                        func.numpy.stddev(n_t.c.d).label('d')])
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(0.005000000000000014, 0.0095742710775633677)]
        )

    def test_unimpl_function(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([func.fake(emp.c.name)])
        assert_raises_message(
            exc.CompileError,
            "Pandas dialect has no 'fake\(\)' function implemented",
            self._exec_stmt, conn, stmt
        )

    def test_order_by_single(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([emp.c.name]).\
                    order_by(emp.c.name)
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('ed', ), ('jack', ), ('wendy', )]
        )

    def test_order_by_multiple_asc(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([emp.c.name, dep.c.name]).\
                    select_from(emp.join(dep)).\
                    order_by(dep.c.name, emp.c.name)
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('jack', 'Accounting'),
                ('ed', 'Engineering'), ('wendy', 'Engineering')]
        )

    def test_order_by_multiple_mixed_one(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([emp.c.name, dep.c.name]).\
                    select_from(emp.join(dep)).\
                    order_by(dep.c.name, emp.c.name.desc())
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('jack', 'Accounting'),
                ('wendy', 'Engineering'), ('ed', 'Engineering')]
        )

    def test_order_by_multiple_mixed_two(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([emp.c.name, dep.c.name]).\
                    select_from(emp.join(dep)).\
                    order_by(dep.c.name.desc(), emp.c.name)
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('ed', 'Engineering'),
                ('wendy', 'Engineering'), ('jack', 'Accounting'), ]
        )

    def test_order_by_expression(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([emp.c.name, dep.c.name]).\
                    select_from(emp.join(dep)).\
                    order_by(dep.c.name + emp.c.name)
        r = self._exec_stmt(conn, stmt)
        # ordering on:   Engineeringed, Engineeringwendy, Accountingjack
        eq_(
            r.fetchall(),
            [('jack', 'Accounting'), ('ed', 'Engineering'),
            ('wendy', 'Engineering')]
        )

    def test_order_by_single_descending(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([emp.c.name]).\
                    order_by(emp.c.name.desc())
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('wendy',), ('jack',), ('ed',)]
        )

    def test_order_by_limit(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([emp.c.name, dep.c.name]).\
                    select_from(emp.join(dep)).\
                    order_by(emp.c.name).\
                    limit(2)
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('ed', 'Engineering'), ('jack', 'Accounting')]
        )

    def test_order_by_offset(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([emp.c.name, dep.c.name]).\
                    select_from(emp.join(dep)).\
                    order_by(emp.c.name).\
                    offset(1)
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('jack', 'Accounting'), ('wendy', 'Engineering')]
        )

    def test_order_by_limit_offset(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([emp.c.name, dep.c.name]).\
                    select_from(emp.join(dep)).\
                    order_by(emp.c.name).\
                    limit(1).offset(1)
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('jack', 'Accounting')]
        )

    def test_order_by_grouped_aggregate(self):
        emp, dep, conn = self._emp_d_fixture()

        stmt = select([func.count(emp.c.name), emp.c.dep_id]).\
                    group_by(emp.c.dep_id).order_by(func.count(emp.c.name))
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(1, 2), (2, 1)]
        )

    def test_group_by_max(self):
        emp, dep, conn = self._emp_d_fixture()

        stmt = select([func.max(emp.c.name), emp.c.dep_id]).\
                    group_by(emp.c.dep_id)
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [('wendy', 1), ('jack', 2)]
        )

    def test_group_by_count(self):
        emp, dep, conn = self._emp_d_fixture()

        stmt = select([func.count(emp.c.name), emp.c.dep_id]).\
                    group_by(emp.c.dep_id)
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(2, 1), (1, 2)]
        )


    def test_group_by_having_col(self):
        emp, dep, conn = self._emp_d_fixture()

        stmt = select([func.count(emp.c.name), emp.c.dep_id]).\
                    group_by(emp.c.dep_id).having(emp.c.dep_id == 2)
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(1, 2)]
        )

    def test_group_by_having_aggregate(self):
        emp, dep, conn = self._emp_d_fixture()

        stmt = select([func.count(emp.c.name), emp.c.dep_id]).\
                    group_by(emp.c.dep_id).having(func.count(emp.c.name) > 1)
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(2, 1)]
        )

    def test_having_group_by_assertion(self):
        emp, dep, conn = self._emp_d_fixture()
        stmt = select([emp.c.dep_id]).having(emp.c.dep_id == 2)
        assert_raises_message(
            dbapi.Error,
            "HAVING must also have GROUP BY",
            self._exec_stmt, conn, stmt
        )
    def test_group_by_expression(self):
        emp, dep, conn = self._emp_d_fixture()

        stmt = select([func.count(emp.c.name), emp.c.name == 'ed']).\
                    group_by(emp.c.name == 'ed')
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(2, False), (1, True)]
        )

    def test_group_by_multiple(self):
        emp, dep, conn = self._emp_d_fixture()

        stmt = select([dep.c.dep_id, dep.c.name, func.count(emp.c.emp_id)]).\
                        select_from(
                            dep.join(emp, dep.c.dep_id == emp.c.dep_id)
                        ).\
                        group_by(dep.c.dep_id, dep.c.name)
        r = self._exec_stmt(conn, stmt)
        eq_(
            r.fetchall(),
            [(1, 'Engineering', 2), (2, 'Accounting', 1)]
        )

class CrudTest(_ExecBase, TestBase):
    def _emp_d_fixture(self, pandas_index_pk=False):
        if pandas_index_pk:
            emp_df = pd.DataFrame(columns=["name", "fullname", "dep_id"])
            dept_df = pd.DataFrame(columns=["name"])
        else:
            emp_df = pd.DataFrame(columns=["emp_id", "name", "fullname", "dep_id"])
            dept_df = pd.DataFrame(columns=["dep_id", "name"])
        m = MetaData()
        emp = Table('employee', m,
                    Column('emp_id', Integer,
                                        primary_key=True),
                    Column('name', String),
                    Column('fullname', String),
                    Column('dep_id', Integer, ForeignKey('department.dep_id')),
                    pandas_index_pk=pandas_index_pk
            )
        dep = Table('department', m,
                    Column('dep_id', Integer, primary_key=True),
                    Column('name', String),
                    pandas_index_pk=pandas_index_pk
                    )
        conn = dbapi.connect(
                        {"employee": emp_df, "department": dept_df})
        return emp, dep, conn

    def _emp_data(self, conn, include_emp_id=True):
        if include_emp_id:
            conn._namespace['employee'] = conn._namespace['employee'].append(
                    pd.DataFrame([
                        {"emp_id": 1, "name": "ed", "fullname": "Ed Jones",
                                "dep_id": 1},
                        {"emp_id": 2, "name": "wendy", "fullname": "Wendy Wharton",
                                "dep_id": 1},
                        {"emp_id": 3, "name": "jack", "fullname": "Jack Smith",
                                "dep_id": 2},
                        ])
                    )
            conn._namespace['department'] = conn._namespace['department'].\
                    append(
                        pd.DataFrame([
                            {"dep_id": 1, "name": "Engineering"},
                            {"dep_id": 2, "name": "Accounting"},
                            {"dep_id": 3, "name": "Sales"},
                        ])
                    )
        else:
            conn._namespace['employee'] = conn._namespace['employee'].append(
                    pd.DataFrame([
                        {"name": "ed", "fullname": "Ed Jones",
                                "dep_id": 0},
                        {"name": "wendy", "fullname": "Wendy Wharton",
                                "dep_id": 0},
                        {"name": "jack", "fullname": "Jack Smith",
                                "dep_id": 1},
                        ])
                    )
            conn._namespace['department'] = conn._namespace['department'].\
                    append(
                        pd.DataFrame([
                            {"name": "Engineering"},
                            {"name": "Accounting"},
                            {"name": "Sales"},
                        ])
                    )

    def test_empty_select(self):
        emp, dep, conn = self._emp_d_fixture(True)
        stmt = select([emp])
        result = self._exec_stmt(conn, stmt)
        eq_(result.fetchall(), [])

    def test_autoincrement_select(self):
        emp, dep, conn = self._emp_d_fixture(pandas_index_pk=True)
        self._emp_data(conn, include_emp_id=False)
        stmt = select([emp])
        result = self._exec_stmt(conn, stmt)
        eq_(result.fetchall(),
            [(0, 'ed', 'Ed Jones', 0), (1, 'wendy', 'Wendy Wharton', 0),
            (2, 'jack', 'Jack Smith', 1)])

    def test_insert_autoincrement(self):
        emp, dep, conn = self._emp_d_fixture(pandas_index_pk=True)
        stmt = emp.insert().values(name='e1', fullname='ef1', dep_id=2)
        result = self._exec_stmt(conn, stmt)
        eq_(result.lastrowid, 0)

        stmt = emp.insert().values(name='e2', fullname='ef2', dep_id=2)
        result = self._exec_stmt(conn, stmt)
        eq_(result.lastrowid, 1)

        stmt = select([emp])
        result = self._exec_stmt(conn, stmt)
        eq_(result.fetchall(),
            [(0, 'e1', 'ef1', 2), (1, 'e2', 'ef2', 2)])

    def test_insert_no_autoincrement(self):
        emp, dep, conn = self._emp_d_fixture(pandas_index_pk=False)
        stmt = emp.insert().values(name='e1', fullname='ef1', dep_id=2)
        result = self._exec_stmt(conn, stmt)
        eq_(result.lastrowid, None)

        stmt = emp.insert().values(name='e2', fullname='ef2', dep_id=2)
        result = self._exec_stmt(conn, stmt)
        eq_(result.lastrowid, None)

        stmt = select([emp])
        result = self._exec_stmt(conn, stmt)
        eq_(result.fetchall(),
            [(None, 'e1', 'ef1', 2), (None, 'e2', 'ef2', 2)])

    def test_insert_multiple_autoincrement(self):
        emp, dep, conn = self._emp_d_fixture(pandas_index_pk=True)
        stmt = emp.insert().values([
                    dict(name='e1', fullname='ef1', dep_id=2),
                    dict(name='e2', fullname='ef2', dep_id=2)
                ])
        result = self._exec_stmt(conn, stmt)
        eq_(result.lastrowid, 1)

        stmt = select([emp])
        result = self._exec_stmt(conn, stmt)
        eq_(result.fetchall(),
            [(0, 'e1', 'ef1', 2), (1, 'e2', 'ef2', 2)])

    def test_simple_update(self):
        emp, dep, conn = self._emp_d_fixture()
        self._emp_data(conn, include_emp_id=True)
        stmt = emp.update().values(fullname='new ef2').where(emp.c.emp_id == 2)
        result = self._exec_stmt(conn, stmt)
        eq_(result.rowcount, 1)

        stmt = select([emp])
        result = self._exec_stmt(conn, stmt)
        eq_(result.fetchall(),
            [(1, 'ed', 'Ed Jones', 1), (2, 'wendy', 'new ef2', 1),
                (3, 'jack', 'Jack Smith', 2)])

    def test_update_cache_clear(self):
        """test SELECT on either side of an UPDATE.

        TableResolver may (or may not, as we adjust implementations)
        be using caching, so this tests
        that it's reset appropriately.

        """
        emp, dep, conn = self._emp_d_fixture()
        self._emp_data(conn, include_emp_id=True)
        self._exec_stmt(conn, select([emp]))

        self._exec_stmt(conn, emp.update().values(fullname='q'))


        result2 = self._exec_stmt(conn, select([emp]))
        eq_(result2.fetchall(),
            [(1, 'ed', 'q', 1), (2, 'wendy', 'q', 1), (3, 'jack', 'q', 2)])

    def test_insert_cache_clear(self):
        """test SELECT on either side of an INSERT.

        TableResolver may (or may not, as we adjust implementations)
        be using caching, so this tests
        that it's reset appropriately.

        """

        emp, dep, conn = self._emp_d_fixture()
        self._emp_data(conn, include_emp_id=True)
        self._exec_stmt(conn, select([emp]))

        self._exec_stmt(conn, emp.insert().values(
                            emp_id=4, name='wanda', fullname='q', dep_id=None))

        result2 = self._exec_stmt(conn,
                            select([emp.c.emp_id, emp.c.name, emp.c.dep_id]))
        eq_(result2.fetchall(),
            [(1, 'ed', 1), (2, 'wendy', 1),
            (3, 'jack', 2), (4, 'wanda', None)])

    def test_expression_update(self):
        emp, dep, conn = self._emp_d_fixture()
        self._emp_data(conn, include_emp_id=True)
        stmt = emp.update().values(fullname=emp.c.name + " smith").\
                    where(or_(emp.c.emp_id == 2, emp.c.emp_id == 3))
        result = self._exec_stmt(conn, stmt)
        eq_(result.rowcount, 2)

        stmt = select([emp])
        result = self._exec_stmt(conn, stmt)
        eq_(result.fetchall(),
            [(1, 'ed', 'Ed Jones', 1), (2, 'wendy', 'wendy smith', 1),
            (3, 'jack', 'jack smith', 2)]
            )

    def test_correlated_subquery_update(self):
        emp, dep, conn = self._emp_d_fixture()
        self._emp_data(conn, include_emp_id=True)

        subq = select([dep.c.name]).where(dep.c.dep_id == emp.c.dep_id).as_scalar()
        stmt = emp.update().values(fullname="dep: " + subq).\
                    where(or_(emp.c.emp_id == 2, emp.c.emp_id == 3))
        result = self._exec_stmt(conn, stmt)
        eq_(result.rowcount, 2)

        stmt = select([emp])
        result = self._exec_stmt(conn, stmt)
        eq_(result.fetchall(),
            [(1, 'ed', 'Ed Jones', 1), (2, 'wendy', 'dep: Engineering', 1),
                (3, 'jack', 'dep: Accounting', 2)])

    def test_simple_delete(self):
        emp, dep, conn = self._emp_d_fixture()
        self._emp_data(conn, include_emp_id=True)

        stmt = emp.delete().where(emp.c.emp_id == 2)
        result = self._exec_stmt(conn, stmt)
        eq_(result.rowcount, 1)

        stmt = select([emp])
        result = self._exec_stmt(conn, stmt)
        eq_(result.fetchall(),
            [(1, 'ed', 'Ed Jones', 1),
                (3, 'jack', 'Jack Smith', 2)])

    def test_delete_all(self):
        emp, dep, conn = self._emp_d_fixture()
        self._emp_data(conn, include_emp_id=True)

        stmt = emp.delete()
        result = self._exec_stmt(conn, stmt)
        eq_(result.rowcount, 3)

        stmt = select([emp])
        result = self._exec_stmt(conn, stmt)
        eq_(result.fetchall(), [])

class CreateDropTest(_ExecBase, TestBase):
    def _conn_fixture(self):
        conn = dbapi.connect(
                        None)
        return conn

    def test_create(self):
        conn = self._conn_fixture()
        m = MetaData()
        t = Table('test', m,
                Column('x', Integer),
                Column('y', Integer)
            )
        self._exec_stmt(conn, schema.CreateTable(t))
        eq_(
            list(conn._namespace['test'].keys()),
            ['x', 'y']
        )

    def test_create_explicit_pk(self):
        conn = self._conn_fixture()
        m = MetaData()
        t = Table('test', m,
                Column('x', Integer, primary_key=True),
                Column('y', Integer)
            )
        self._exec_stmt(conn, schema.CreateTable(t))
        eq_(
            list(conn._namespace['test'].keys()),
            ['x', 'y']
        )

    def test_create_index_pk(self):
        conn = self._conn_fixture()
        m = MetaData()
        t = Table('test', m,
                Column('x', Integer, primary_key=True),
                Column('y', Integer),
                pandas_index_pk=True
            )
        self._exec_stmt(conn, schema.CreateTable(t))
        eq_(
            list(conn._namespace['test'].keys()),
            ['y']
        )

    def test_drop(self):
        conn = self._conn_fixture()
        m = MetaData()
        t = Table('test', m,
                Column('x', Integer),
                Column('y', Integer)
            )
        self._exec_stmt(conn, schema.CreateTable(t))
        self._exec_stmt(conn, schema.DropTable(t))
        assert 'test' not in conn._namespace

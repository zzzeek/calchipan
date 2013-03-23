from sqlalchemy.testing.fixtures import TestBase

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, \
    String, Integer, ForeignKey, select, exc, func
from . import eq_, assert_raises_message

class DialectTest(TestBase):

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
        return {"employee": emp_df, "department": dept_df}

    def _md_fixture(self):
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
        return emp, dep

    def test_has_table(self):
        eng = create_engine("pandas+calhipan://",
                    namespace=self._emp_d_fixture())
        assert eng.has_table('employee')
        assert not eng.has_table('fake')

    def test_execute_compiled(self):
        eng = create_engine("pandas+calhipan://",
                    namespace=self._emp_d_fixture())
        emp, dep = self._md_fixture()
        result = eng.execute(select([emp]))
        eq_(
            list(result),
            [(1, 'ed', 'Ed Jones', 1),
                (2, 'wendy', 'Wendy Wharton', 1),
                (3, 'jack', 'Jack Smith', 2)]
        )

    def test_execute_standalone_fn(self):
        eng = create_engine("pandas+calhipan://",
                    namespace=self._emp_d_fixture())
        emp, dep = self._md_fixture()

        from sqlalchemy.sql.functions import GenericFunction
        class MyFunc(GenericFunction):
            name = 'myfunc'
            def pandas_fn(self, arg):
                return pd.Series(["hi"])
        result = eng.execute(func.myfunc())
        eq_(
            list(result),
            [('hi',)]
        )

    def test_no_ddl(self):
        eng = create_engine("pandas+calhipan://",
                    namespace=self._emp_d_fixture())
        m = MetaData()
        Table('t', m, Column('x', Integer))

        assert_raises_message(
            exc.StatementError,
            "Only Pandas-compiled statements can be executed by this dialect",
            m.create_all, eng
        )
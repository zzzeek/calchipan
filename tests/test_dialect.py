from sqlalchemy.testing.fixtures import TestBase

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, \
    String, Integer, ForeignKey, select, exc, func
from . import eq_, assert_raises_message

class DialectTest(TestBase):

    def _emp_d_fixture(self, id_cols=True):
        if id_cols:
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
        else:
            emp_df = pd.DataFrame([
                    {"name": "ed", "fullname": "Ed Jones",
                            "dep_id": 0},
                    {"name": "wendy", "fullname": "Wendy Wharton",
                            "dep_id": 0},
                    {"name": "jack", "fullname": "Jack Smith",
                            "dep_id": 1},

                ])
            dept_df = pd.DataFrame([
                        {"name": "Engineering"},
                        {"name": "Accounting"},
                        {"name": "Sales"},
                        ])

        return {"employee": emp_df, "department": dept_df}

    def _md_fixture(self, pandas_index_pk=False):
        m = MetaData()
        emp = Table('employee', m,
                    Column('emp_id', Integer, primary_key=True),
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
        return emp, dep

    def test_has_table(self):
        eng = create_engine("pandas+calchipan://",
                    namespace=self._emp_d_fixture())
        assert eng.has_table('employee')
        assert not eng.has_table('fake')

    def test_execute_compiled(self):
        eng = create_engine("pandas+calchipan://",
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
        eng = create_engine("pandas+calchipan://",
                    namespace=self._emp_d_fixture())
        emp, dep = self._md_fixture()

        from calchipan import non_aggregate_fn

        @non_aggregate_fn()
        def myfunc():
            return "hi"
        result = eng.execute(myfunc())
        eq_(
            list(result),
            [('hi',)]
        )

    def test_inserted_primary_key_autoinc(self):
        eng = create_engine("pandas+calchipan://",
                    namespace={"employee":
                            pd.DataFrame(columns=["name", "dep_id"])})
        emp, dep = self._md_fixture(pandas_index_pk=True)
        r = eng.execute(emp.insert().values(name="ed", dep_id=5))
        eq_(r.inserted_primary_key, [0])

        r = eng.execute(emp.insert().values(name="jack", dep_id=12))
        eq_(r.inserted_primary_key, [1])

    def test_inserted_primary_key_no_autoinc(self):
        eng = create_engine("pandas+calchipan://",
                    namespace={"employee":
                            pd.DataFrame(columns=["name", "dep_id"])})
        emp, dep = self._md_fixture(pandas_index_pk=False)
        r = eng.execute(emp.insert().values(name="ed", dep_id=5))
        eq_(r.inserted_primary_key, [None])

        r = eng.execute(emp.insert().values(name="jack", dep_id=12))
        eq_(r.inserted_primary_key, [None])

    def test_inserted_primary_key_manualinc(self):
        eng = create_engine("pandas+calchipan://",
                    namespace={"employee":
                            pd.DataFrame(columns=["emp_id", "name", "dep_id"])})
        emp, dep = self._md_fixture()
        r = eng.execute(emp.insert().values(emp_id=3, name="ed", dep_id=5))
        eq_(r.inserted_primary_key, [3])




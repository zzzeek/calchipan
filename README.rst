============================================
Crouching Alchemist Hidden Panda (CALCHIPAN)
============================================

What is it?
===========

A `SQLAlchemy <http://www.sqlalchemy.org/>`_ dialect which will
consider a series of `Pandas <pandas.pydata.org/>`_ DataFrame objects
as relational database tables, allowing them to be queried and manipulated
in all the usual SQLAlchemy ways.

Why is it?
==========

`Me <https://twitter.com/zzzeek/status/313405747134357505>`_: Naive Pandas idea #1: map SQLAlchemy declarative classes to pandas dataframes, use ORM or core queries to query it. Pointless?

`Wes McKinney <https://twitter.com/wesmckinn/status/313412273043537920>`_: @chriswithers13 @zzzeek integration layers = good

From the man himself, and a Pycon sprint project was born!

Example
=======

::

  from sqlalchemy import create_engine, MetaData, Table, Column, \
      Integer, select, String

  engine = create_engine("pandas://")

  m = MetaData()
  employee = Table('emp', m,
      Column('emp_id', Integer, primary_key=True),
      Column('name', String)
    )
  m.create_all(engine)
  engine.execute(employee.insert().values(emp_id=1, name='ed'))

  result = engine.execute(
          select([employee]).where(employee.c.name == 'ed')
        )

  print(result.fetchall())

Note we didn't need to import Pandas at all above; we created
and populated the table entirely using SQLAlchemy means,
and Pandas remained hidden as a storage engine - hence the name!

Of course, you can and in most cases probably should start with
regular Pandas dataframes, and send them off into an engine
to be queried.  Using this approach, the Table objects can
be built explicitly or more easily just reflected as below;
obviously primary/foreign key constraints, useful for when
using the ORM, would still have to be established
manually::

  import pandas as pd

  dept_df = pd.DataFrame([{"dep_id": 1, "name": "Engineering"},
                      {"dep_id": 2, "name": "Accounting"}])

  emp_df = pd.DataFrame([
      {"emp_id": 1, "name": "ed", "fullname": "Ed Jones", "dep_id": 1},
      {"emp_id": 2, "name": "wendy", "fullname": "Wendy Wharton", "dep_id": 1}
      ])

  engine = create_engine("pandas://",
              namespace={"dept": dept_df, "emp": emp_df})

  m = MetaData()
  department = Table('dept', m, autoload=True, autoload_with=engine)
  employee = Table('emp', m, autoload=True, autoload_with=engine)

  result = engine.execute(
              select([employee.c.name, department.c.name]).
                  select_from(
                          employee.join(department,
                              employee.c.dep_id == department.c.dep_id))
              )
  print(result.fetchall())

Add Any Python Function
=======================

Since we're totally in Python now, you can make any kind of Python function
into a SQL function, most handily the various numpy functions::

    from calchipan import aggregate_fn
    @aggregate_fn(package='numpy')
    def stddev(values):
        return values.std()

Above, ``stddev`` is now a SQLAlchemy aggregate function::

  result = engine.execute(select([stddev(mytable.c.data)]))

And it is also available from the ``func`` namespace (note we
also put it into a sub-"package" called ``numpy``::

  from sqlalchemy import func
  result = engine.execute(select([func.numpy.stddev(mytable.c.data)]))

Great, so Pandas is totally SQL-capable now right?
==================================================

Well let's just slow down there.   The library here is actually
working quite nicely, and yes, you can do a pretty decent range of SQL operations
here, noting the caveats that **this is super duper alpha stuff I just started a week ago**.
Some SQL operations that we normally take for granted will **perform pretty badly**
(guide is below), so at the moment it's not entirely clear how much speed will be
an issue.  There's a good number of tests for all the SQL functionality
that's been implemented, though these are all rudimentary "does it work at all"
tests dealing with at most only three or four rows and two tables.
Additional functional tests with real world ORM examples have shown very good
results, illustrating queries with fairly complex geometries (lots of subqueries,
aliases, and joins) working very well with no errors.  The performance
of some operations, particularly data mutation operations, are
fairly slow, but Pandas is not oriented
towards manipulation of DataFrames in a CRUD-style way in any case.
For straight up SELECTs that stay close to primary Pandas use cases, results
should be pretty decent.

Can I just type "select * from table" and it will work?
=======================================================================

No, we're dealing here strictly with
`SQLAlchemy expression constructs <http://docs.sqlalchemy.org/en/rel_0_8/core/tutorial.html>`_
as the source of the SQL parse tree.   So while the
`ORM <http://docs.sqlalchemy.org/en/rel_0_8/orm/tutorial.html>`_ works just fine,
there's no facility here to actually receive a SQL string itself.
However, the (more) ambitious (than me)
programmer should be able to take a product like `sqlparse <http://code.google.com/p/python-sqlparse/>`_
and use that product's parse tree to deliver the same command objects that the compiler does here,
the ``calchipan.compiler`` (SQLAlchemy compiler) and ``calchipan.resolver`` (command objects understood
by the Pandas DBAPI) are entirely separate, and the resolver has minimal dependencies on
SQLAlchemy.

All your caveats and excuses are making me sad.
===============================================

Here's the `pandasql <https://github.com/yhat/pandasql>`_ package, which does basically
the same thing that `sqldf <http://code.google.com/p/sqldf/>`_ does for R, which is copies data out
to SQLite databases as needed and lets you run SQL against that.   So if you want
straight up SQL queries delivered perfectly, use that.  You just have to wait while it copies
all your dataframes out to the database for each table (which might not be a problem at all).
pandasql also doesn't provide easy hooks for usage with packages like SQLAlchemy, though the whole
thing is only 50 lines so hacking its approach might be worth it.

Will CALCHIPAN at least return the right results to me?
========================================================

As noted before, initial testing looks very good.  But note that this is
half of a relational database implementation written in Python; if you look at
`sqlite's changelog <http://www.sqlite.org/releaselog/3_7_16.html>`_ you can see they
are still fixing "I got the wrong answer" types of bugs after **nine years of
development**, which is 46800% the number of weeks versus Calchipans one week
of development time as of March 25, 2013.  So as a rule of thumb I'd
say **Calchipan is way too new to be trusted with anything at all.**
Feel free to use the bugtracker here to report on early usage experiences
and issues, the latter should absolutely be expected.

Performance Notes
==================

The SQL operations are all implemented with an emphasis
on relying upon Pandas in the simplest and most idiomatic way possible for any
query given.  Two common SQL operations,
implicit joins and correlated subqueries, work fully, but are not optimized at all -
an implicit join (that is, selecting from more than one table without using ``join()``)
relies internally on producing a `cartesian product <http://en.wikipedia.org/wiki/Cartesian_product>`_,
which you aren't going to like for large (or even a few thousand rows) datasets.
Correlated subqueries involve
running the subquery individually on every row, so these will also make
the speed-hungry user sad (but the "holy crap correlated subqueries are possible with Pandas?"
user should be really happy!).   A join using ``join()`` or ``outerjoin()`` will internally
make use of Pandas' ``merge()`` function directly for simple criteria, so if you
stay within the lines, you should get pretty good Pandas-like performance, but if you
try non-simple criteria like joinining on "x > y", you'll be back in
cartesian land.

The libary also does a little bit of restatement of dataframes internally which has a
modest performance hit, which is more significant if one is using the "index as primary key"
feature, which involves making copies of the DataFrame's index into a column.

What's Implemented
===================

* ``select()``

  * WHERE criterion
  * column expressions, functions
  * implicit joins (where multiple tables are specified without using JOIN)
  * explicit joins (i.e. using join()), on simple criteria (fast) and custom criteria (slower)
  * explicit outerjoins (using outerjoin()), on simple criteria (sort of fast)
    and custom criteria (slower)
  * subqueries in the FROM clause
  * subqueries in the columns and WHERE clause which can be correlated; note that column/where
    queries are not very performant however as they invoke explicitly for every row in the
    parent result
  * ORDER BY
  * GROUP BY
  * aggregate functions, including custom user-defined aggregate functions
  * HAVING, including comparison of aggregate function values
  * LIMIT, using ``select().limit()``
  * OFFSET, using ``select().offset()``
  * UNION ALL, using ``union_all()``
  * A few SQL functions are implemented so far, including ``count()``, ``max()``, ``min()``, and ``now()``

* Table reflection

  * Only gets the names of columns, and at best only the "String", "Integer", "Float"
    types based on a dataframe.   There's no primary key, foreign key constraints,
    defaults, indexes or anything like that.  Primary and FK constraints would need
    to be specified to the ``Table()`` explicitly if one is using the ORM and
    wishes these constructs to be present.

* CRUD operations - Note that Pandas **is not** optimized for modifications of dataframes,
  and dataframes should normally be populated ahead of time using normal Pandas APIs,
  unless SQL-specific or ORM-specific functionality is needed.
  CRUD operations here work correctly but are not by any means fast, nor is there any
  notion of thread safety or anything like that.   ORM models can be fully persisted
  to dataframes using this functionality.

  * ``insert()``

    * Plain inserts
    * multi-valued inserts, i.e. ``table.insert().values([{"a": 1, "b": 2}, {"a": 3, "b": 4}])``
    * Note that inserts here must create a new dataframe for each statement invoked!
      Generally, dataframes should be populated using Pandas standard methods; INSERT here
      is only a utility
    * cursor.lastrowid - if the table is set up to use the Pandas "index" as the primary key,
      this value will function.   The library is less efficient when used in this mode,
      however, as it needs to copy the index column every time the table is accessed.
      SQLAlchemy returns this value as result.inserted_primary_key().

  * ``update()``

    * Plain updates
    * Expression updates, i.e. set the value of a column to an expression
      possibly deriving from other columns in the row
    * Correlated subquery updates, i.e. set the value of a column to
      the result of a correlated subquery
    * Full WHERE criterion including correlated subqueries
    * cursor.rowcount, number of rows matched.

  * ``delete()``

    * Plain deletes
    * Full WHERE criterion including correlated subqueries
    * cursor.rowcount, number of rows matched

* ORM

  * The SQLAlchemy ORM builds entirely on top of the Core SQL constructs above, so
    it works fully.

What's Egregiously Missing
===========================

* Other set ops besides UNION ALL - UNION, EXCEPT, INTERSECTION, etc., these should
  be easy to implement
* RETURNING for insert, update, delete, also should be straightforward to implement
* Lots of obvious functions are missing, only a few are present so far
* Coercion/testing of Python date and time values.  Pandas seems to use an internal
  Timestamp format, so SQLAlchemy types that coerce to/from Python datetime() objects
  and such need to be added.
* EXISTS, needs to be evaluated
* CASE statements (should be very easy)
* anything fancy, window functions, CTEs, etc.

* **ANY KIND OF INPUT SANITIZING** - I've no idea if Pandas and/or numpy have any kind
  of remote code execution vulnerabilities, but if they do, **they are here as well**.
  **This library has no security features of any kind, please do not send untrusted
  data into it**.

Thanks, and have a nice day!

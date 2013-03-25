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
			Integer, select

	engine = create_engine("pandas://")

	m = MetaData()
	t1 = Table('t1', m,
				Column('x', Integer, primary_key=True),
				Column('y', Integer)
			)
	m.create_all(engine)
	engine.execute(t1.insert().values(x=1, y=2))

	result = engine.execute(select([t1]).where(t1.c.x == 1))

	result.fetchall()


The engine can be passed a series of existing dataframes, and
rudimentary table reflection works too (obviously primary/foreign keys
would have to be established manually)::

	engine = create_engine("pandas://",
				namespace={"df1": dataframe, "df2": otherdataframe})

	m = MetaData()
	df1 = Table('df1', m, autoload=True, autoload_with=engine)
	df2 = Table('df1', m, autoload=True, autoload_with=engine)


Great, so Pandas is totally SQL-capable now right?
==================================================

Well let's just slow down there.   The library here is actually
working quite nicely, and yes, you can do a pretty decent range of SQL operations
here, noting the caveats that **this is super duper alpha stuff I just started a week ago**.
Many of the SQL operations that we take for granted will **perform pretty badly**
(guide is below), so at the moment it's not entirely clear how useful this approach
will really be - I have comprehensive tests for all the SQL operations implemented,
but these are only rudimentary "does it work at all" tests - dealing
with at most about six rows and just two tables.   I've run some more real world ORM
types of scripts and the performance is a little slow, but then again Pandas is not oriented
towards manipulation of DataFrames in a CRUD-style way so for straight up SELECTs
it may be quite useful for some people.

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

Performance Notes
==================

The SQL operations are all implemented in the simplest way possible, with an emphasis
on returning the correct answer for any query given.  Two common SQL operations,
implicit joins and correlated subqueries, work fully, but are not optimized at all -
an implicit join (that is, selecting from more than one table without using ``join()``)
relies internally on producing a `cartesian product <http://en.wikipedia.org/wiki/Cartesian_product>`_,
which you aren't going to like for large datasets.   Correlated subqueries involve
running the correlated query individually on every row, so these will also make
the speed-hungry user sad.   A join using ``join()`` or ``outerjoin()`` will internally
try to make use of Pandas' ``merge()`` function directly, but this only takes effect
for simple criteria - if you try to join on a condition like "x > y", you'll be back in
cartesian land.

The libary also does a little bit of restatement of dataframes internally which has a
modest performance hit, which is more significant if one is using the "index as primary key"
feature, which involves making copies of the DataFrame's index into a column.

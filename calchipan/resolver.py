
"""Represent SQL tokens as Pandas operations.

"""
from sqlalchemy.sql import operators
from sqlalchemy import sql
from sqlalchemy import util
from sqlalchemy import types as sqltypes
import functools
import pandas as pd
import numpy as np
import collections
from . import dbapi
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.ext.compiler import compiles


def aggregate_fn(package=None):
    """Mark a Python function as a SQL aggregate function.

    The function should typically receive a Pandas Series object
    as an argument and return a scalar result.

    E.g.::

        from calchipan import aggregate_fn

        @aggregate_fn()
        def stddev(values):
            return values.std()

    The object is converted into a SQLAlchemy GenericFunction
    object, which can be used directly::

        stmt = select([stddev(table.c.value)])

    or via the SQLAlchemy ``func`` namespace::

        from sqlalchemy import func
        stmt = select([func.stddev(table.c.value)])

    Functions can be placed in ``func`` under particular
    "package" names using the ``package`` argument::

        @aggregate_fn(package='numpy')
        def stddev(values):
            return values.std()

    Usage via ``func`` is then::

        from sqlalchemy import func
        stmt = select([func.numpy.stddev(table.c.value)])

    An aggregate function that is called with multiple expressions
    will be passed a single argument that is a list of Series
    objects.

    """
    def mark_aggregate(fn):
        kwargs = {'name': fn.__name__}
        if package:
            kwargs['package'] = package
        custom_func = type("%sFunc" % fn.__name__, (GenericFunction,), kwargs)

        @compiles(custom_func, 'pandas')
        def _compile_fn(expr, compiler, **kw):
            return FunctionResolver(fn,
                    compiler.process(expr.clauses, **kw), True)
        return custom_func
    return mark_aggregate

def non_aggregate_fn(package=None):
    """Mark a Python function as a SQL non-aggregate function.

    The function should receive zero or more scalar
    Python objects as arguments and return a scalar result.

    E.g.::

        from calchipan import non_aggregate_fn

        @non_aggregate_fn()
        def add_numbers(value1, value2):
            return value1 + value2

    Usage and behavior is identical to that of :func:`.aggregate_fn`,
    except that the function is not treated as an aggregate.  Function
    expressions are also expanded out to individual positional arguments,
    whereas an aggregate always receives a single structure as an argument.

    """
    def mark_non_aggregate(fn):
        kwargs = {'name': fn.__name__}
        if package:
            kwargs['package'] = package
        custom_func = type("%sFunc" % fn.__name__, (GenericFunction,), kwargs)

        @compiles(custom_func, 'pandas')
        def _compile_fn(expr, compiler, **kw):
            return FunctionResolver(fn,
                    compiler.process(expr.clauses, **kw), False)
        return custom_func
    return mark_non_aggregate

ResolverContext = collections.namedtuple("ResolverContext",
                            ["cursor", "namespace", "params"])

class Resolver(object):
    def __call__(self, cursor, namespace, params):
        """Resolve this expression.

        Resolvers are callables; this is called by the DBAPI."""
        return self.resolve(ResolverContext(cursor, namespace, params))

    def resolve(self, ctx):
        """Resolve this expression given a ResolverContext.

        Front end for resolution, linked to top-level __call__()."""
        raise NotImplementedError()

class NullResolver(Resolver):
    def resolve(self, ctx):
        pass

class ColumnElementResolver(Resolver):
    """Top level class for SQL expressions."""

    def resolve_expression(self, ctx, product):
        """Resolve as a column expression.

        Return value here is typically a Series or a scalar
        value.

        """
        raise NotImplementedError()

class FromResolver(Resolver):
    """Top level class for 'from' objects, things you can select rows from."""

    def resolve_dataframe(self, ctx, names=True):
        """Resolve as a dataframe.

        Return value here is a DataFrame object.

        """
        raise NotImplementedError()

class FunctionResolver(ColumnElementResolver):
    def __init__(self, fn, expr, aggregate):
        self.fn = fn
        self.expr = expr
        self.aggregate = aggregate

    def resolve_expression(self, ctx, product):
        if self.aggregate:
            q = self.fn(self.expr.resolve_expression(
                    ctx, product))
            q = pd.Series([q], name="aggregate")
        else:
            q = self.fn(*self.expr.resolve_expression(
                    ctx, product))
        return q

class ConstantResolver(ColumnElementResolver):
    def __init__(self, value):
        self.value = value

    def resolve_expression(self, ctx, product):
        return self.value

class LiteralResolver(ColumnElementResolver):
    def __init__(self, value):
        self.value = value
        self.name = str(id(self))

    def resolve_expression(self, ctx, product):
        return self.value

    @property
    def df_index(self):
        return self.name

class ColumnResolver(ColumnElementResolver):
    def __init__(self, name, tablename):
        self.name = name
        self.tablename = tablename

    def resolve_expression(self, ctx, product):
        if product is None:
            df = TableResolver(self.tablename).resolve_dataframe(ctx)
        else:
            df = product.resolve_dataframe(ctx)
        return df[self.df_index]

    @property
    def df_index(self):
        return "#T_%s_#C_%s" % (self.tablename, self.name)

class UnaryResolver(ColumnElementResolver):
    def __init__(self, expression, operator, modifier):
        self.operator = operator
        self.modifier = modifier
        self.expression = expression

    def resolve_expression(self, ctx, product):
        return self.expression.resolve_expression(
                            ctx, product)

    @property
    def df_index(self):
        return self.expression.df_index

class LabelResolver(Resolver):
    def __init__(self, expression, name):
        self.expression = expression
        self.name = name

    def resolve_expression(self, ctx, product):
        return self.expression.resolve_expression(ctx, product)

    @property
    def df_index(self):
        return self.name


class BinaryResolver(ColumnElementResolver):
    def __init__(self, left, right, operator):
        self.left = left
        self.right = right
        self.operator = operator

    def resolve_expression(self, ctx, product):
        return self.operator(
                    self.left.resolve_expression(ctx, product),
                    self.right.resolve_expression(ctx, product),
                )

class ClauseListResolver(ColumnElementResolver):
    def __init__(self, expressions, operator):
        self.expressions = expressions
        self.operator = operator

    def resolve_expression(self, ctx, product):
        exprs = [expr.resolve_expression(ctx, product)
                        for expr in self.expressions]

        if self.operator is operators.comma_op:
            if len(exprs) == 1:
                return exprs[0]
            else:
                return exprs
        else:
            return functools.reduce(self.operator, exprs)

class BindParamResolver(ColumnElementResolver):
    def __init__(self, name):
        self.name = name

    def resolve_expression(self, ctx, product):
        return ctx.params[self.name]


class DerivedResolver(FromResolver):
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def resolve_dataframe(self, ctx, names=True):
        return self.dataframe


class TableResolver(FromResolver):
    def __init__(self, tablename, autoincrement_col=None):
        self.tablename = tablename
        self.autoincrement_col = autoincrement_col

    def resolve_dataframe(self, ctx, names=True):
        df = ctx.namespace[self.tablename]
        if names:
            # performance tests show that the rename() here is
            # not terribly expensive as long as copy=False.  Adding the
            # index as a column is much more expensive, however,
            # though is not as common of a use case.

            # the renamed dataframe can be cached, though this means
            # that all mutation operations need to clear the cache also.

            # a quicker route to having the index accessible is to
            # add an explicit copy of the index to the DataFrame outside
            # of the SQL dialect - that way it won't be copied here
            # each time.

            renamed_df = df.rename(
                        columns=dict(
                            (k, "#T_%s_#C_%s" % (self.tablename, k))
                            for k in df.keys()
                        ), copy=False
                    )
            if self.autoincrement_col and self.autoincrement_col not in df:
                renamed_df["#T_%s_#C_%s" %
                        (self.tablename, self.autoincrement_col)] = df.index
            return renamed_df
        elif self.autoincrement_col and self.autoincrement_col not in df:
            renamed_df = df.copy()
            renamed_df[self.autoincrement_col] = df.index
            return renamed_df

        else:
            return df

class AliasResolver(FromResolver):
    def __init__(self, table, aliasname):
        self.table = table
        self.aliasname = aliasname

    def resolve_dataframe(self, ctx, names=True):
        df = self.table.resolve_dataframe(ctx, names=False)
        if names:
            df = df.rename(
                        columns=dict(
                            (k, "#T_%s_#C_%s" % (self.aliasname, k))
                            for k in df.keys()
                        ), copy=False
                    )
        return df

class JoinResolver(FromResolver):
    def __init__(self, left, right, onclause, isouter):
        self.left = left
        self.right = right
        self.onclause = onclause
        self.isouter = isouter

    def resolve_dataframe(self, ctx, names=True):
        df1 = left = self.left.resolve_dataframe(ctx)
        df2 = self.right.resolve_dataframe(ctx)

        if self.isouter:
            left['_cp_left_index'] = left.index

        straight_binaries, remainder = self._produce_join_expressions(df1, df2)

        df1 = self._merge_straight_binaries(ctx, df1, df2, straight_binaries)

        df1 = self._merge_remainder(ctx, left, df1, df2,
                                    straight_binaries, remainder)
        return df1.where(pd.notnull(df1), None)

    def _produce_join_expressions(self, df1, df2):
        straight_binaries = []
        remainder = []
        if isinstance(self.onclause, ClauseListResolver) and \
                self.onclause.operator is operators.and_:
            comparisons = self.onclause.expressions
        else:
            comparisons = [self.onclause]

        # extract comparisons like this:
        # col1 == col2 AND col3 == col4 AND ...
        # use pd.merge() for those
        for comp in comparisons:
            if isinstance(comp, BinaryResolver) and \
                comp.operator is operators.eq and \
                hasattr(comp.left, "df_index") and \
                    hasattr(comp.right, "df_index"):

                if comp.left.df_index in df1 and \
                        comp.right.df_index in df2:
                    straight_binaries.append(
                        (comp.left.df_index, comp.right.df_index)
                    )
                    continue
                elif comp.right.df_index in df1 and \
                        comp.left.df_index in df2:
                    straight_binaries.append(
                        (comp.right.df_index, comp.left.df_index)
                    )
                    continue

            remainder.append(comp)
        return straight_binaries, remainder

    def _merge_straight_binaries(self, ctx, df1, df2, straight_binaries):
        if straight_binaries:
            # use merge() for straight binaries.
            left_on, right_on = zip(*straight_binaries)
            df1 = df1.merge(df2, left_on=left_on, right_on=right_on,
                            how='left' if self.isouter else 'inner')
        return df1

    def _merge_remainder(self, ctx, left, df1, df2,
                            straight_binaries, remainder):
        # for joins that aren't straight "col == col",
        # we use the ON criterion directly.
        # if we don't already have a dataframe with the full
        # left + right cols, we use a cartesian product first.
        # ideally, we'd limit the cartesian on only those columns we
        # need.
        if remainder:
            if len(remainder) > 1:
                remainder = ClauseListResolver(remainder, operators.and_)
            else:
                remainder = remainder[0]

            # TODO: performance optimization: do the cartesian product
            # here on a subset of the two dataframes, that only includes
            # those columns we need in the expression.   Then reindex
            # back out to the original dataframes.
            if not straight_binaries:
                df1 = _cartesian_dataframe(ctx, df1, df2)
            expr = remainder.resolve_expression(ctx, DerivedResolver(df1))

            joined = df1[expr]

            if self.isouter:
                # for outer join, grab remaining rows from "left"
                remaining_left_ids = set(df1['_cp_left_index']).\
                                        difference(joined['_cp_left_index'])
                remaining = left.ix[remaining_left_ids]
                df1 = pd.concat([joined, remaining]).reset_index()
            else:
                df1 = joined
        return df1


class _ExprCol(ColumnElementResolver):
    def __init__(self, expr, name):
        self.expr = expr
        self.name = name

    def resolve_expression(self, ctx, product):
        return self.expr.resolve_expression(ctx, product)

    @property
    def df_index(self):
        return self.name

class BaseSelectResolver(FromResolver):
    group_by = None
    order_by = None
    having = None
    limit = None
    offset = None

    @util.memoized_property
    def columns(self):
        return []

    def _evaluate(self, ctx, correlate=None):
        raise NotImplementedError()

    def resolve(self, ctx, correlate=None):
        product = self._evaluate(ctx, correlate)

        if self.group_by is not None:
            df = product.resolve_dataframe(ctx)
            gp = self.group_by.resolve_expression(ctx, product)
            groups = [DerivedResolver(gdf[1]) for gdf in df.groupby(gp)]
        else:
            groups = [product]

        frame_columns = list(self.columns)

        if self.having is not None:
            if self.group_by is None:
                raise dbapi.Error("HAVING must also have GROUP BY")
            frame_columns.append(_ExprCol(self.having, '_having'))

        if self.order_by is not None:
            for idx, ob_expr in enumerate(self.order_by.expressions):
                frame_columns.append(_ExprCol(ob_expr, '_order_by_%d' % idx))

        def process_aggregates(gprod):
            """detect aggregate functions in column clauses and
            flatten results if present
            """
            cols = [
                    _coerce_to_series(
                        ctx,
                        c.resolve_expression(ctx, gprod)
                    ).reset_index(drop=True)
                    for c in frame_columns]


            for c in cols:
                if c.name == 'aggregate':
                    break
            else:
                return cols

            return [
                list(c)[0]
                    if c.name != 'aggregate'
                    else c
                for c in cols
            ]

        nu = _unique_name()
        names = [nu(c.name) for c in self.columns]

        group_results = [
            pd.DataFrame.from_items(
                    [
                        (
                            c.df_index,
                            expr
                        )
                        for c, expr
                            in zip(frame_columns, process_aggregates(gprod))
                    ]
            )
            for gprod in groups
        ]

        non_empty = [g for g in group_results if len(g)]
        if not non_empty:
            # empty result
            return pd.DataFrame(columns=names)
        else:
            results = pd.concat(non_empty)

        if self.having is not None:
            results = results[results['_having'] == True]
            del results['_having']

        if self.order_by:
            cols = []
            asc = []
            for idx, ob_expr in enumerate(self.order_by.expressions):
                ascending = \
                    not isinstance(ob_expr, UnaryResolver) or \
                    ob_expr.modifier is not operators.desc_op
                key = '_order_by_%d' % idx
                cols.append(key)
                asc.append(ascending)
            results = results.sort(columns=cols, ascending=asc).\
                            reset_index(drop=True)
            for col in cols:
                del results[col]

        results.rename(columns=dict(
                        (col.df_index, name)
                        for col, name in zip(self.columns, names)
                        ), inplace=True)

        if self.offset is not None or self.limit is not None:
            slice_start = self.offset if self.offset is not None else 0
            if self.limit is None:
                results = results[slice_start:]
            else:
                results = results[slice_start:slice_start + self.limit]
        return results

class SelectResolver(BaseSelectResolver):
    whereclause = None

    @util.memoized_property
    def dataframes(self):
        return []


    def resolve_dataframe(self, ctx, names=True):
        return self.resolve(ctx)

    def resolve_expression(self, ctx, product):
        # correlated subquery - resolve for every row.
        # TODO: probably *dont* need to resolve for every row
        # for an uncorrelated subquery, can detect that
        p_df = product.resolve_dataframe(ctx)

        # iterate through rows in dataframe and form one-row
        # dataframes.  The ind:ind thing is the only way I could
        # figure out to achieve this, might be an eaiser way.
        things = []
        for ind in p_df.index:
            row = p_df.ix[ind:ind]
            df = DerivedResolver(row)
            thing = self._evaluate(ctx, correlate=df)

            things.append(_coerce_to_scalar(ctx, thing))
        return pd.Series(things)

    def _evaluate(self, ctx, correlate=None):
        if not self.dataframes:
            # "null" dataframe
            product = DerivedResolver(pd.DataFrame(
                        [{col.df_index: [1]} for col in self.columns]))
        else:
            product = self.dataframes[0]
        for df in self.dataframes[1:]:
            product = _cartesian(ctx, product, df)
        if correlate:
            product = _cartesian(ctx, product, correlate)
        df = product.resolve_dataframe(ctx)
        if self.whereclause is not None:
            df = df[self.whereclause.resolve_expression(ctx, product)]

        product = DerivedResolver(df)
        if correlate:
            col = self.columns[0].resolve_expression(ctx, product)
            return _coerce_to_scalar(ctx, col)

        return product


class CompoundResolver(BaseSelectResolver):
    keyword = None

    @util.memoized_property
    def selects(self):
        return []

    @property
    def columns(self):
        return self.selects[0].columns

    def resolve_dataframe(self, ctx, names=True):
        return self.resolve(ctx)

    def _evaluate(self, ctx, correlate=None, **kw):
        assert self.keyword in (sql.CompoundSelect.UNION,
                                sql.CompoundSelect.UNION_ALL)

        evaluated = [
            sel.resolve(ctx, **kw)
            for sel in self.selects
        ]

        for ev in evaluated:
            ev.rename(columns=dict(
                    (old, new.df_index) for old, new in
                    zip(ev.keys(), self.columns)
                ),
                inplace=True)

        df = pd.concat(evaluated)
        if self.keyword == sql.CompoundSelect.UNION:
            df = df.drop_duplicates()
        return DerivedResolver(df)

class CRUDResolver(Resolver):
    pass

class InsertResolver(CRUDResolver):
    columns = ()
    values = ()

    def __init__(self, tablename, pandas_index_pk):
        self.tablename = tablename
        self.pandas_index_pk = pandas_index_pk

    def resolve(self, ctx, **kw):
        df = ctx.namespace[self.tablename]
        if not self.values:
            new = df.append({}, ignore_index=True)
        elif isinstance(self.values[0], list):
            new = df.append(
                pd.DataFrame(
                    [
                        dict((c,
                            v.resolve_expression(ctx, None))
                            for c, v in zip(self.columns, row))
                        for row in self.values
                    ]
                ), ignore_index=True
            )
        else:
            new = df.append(dict(
                    (c, v.resolve_expression(ctx, None))
                    for c, v in zip(self.columns, self.values)
                ), ignore_index=True)

        # TODO: is 'value=[None]' correct usage here?
        ctx.namespace[self.tablename] = new.fillna(value=[None])
        if self.pandas_index_pk:
            ctx.cursor.lastrowid = new.index[-1]
        else:
            ctx.cursor.lastrowid = None

class UpdateResolver(CRUDResolver):
    values = ()
    whereclause = None

    def __init__(self, tablename, autoincrement_col):
        self.tablename = tablename
        self.autoincrement_col = autoincrement_col

    def resolve(self, ctx, **kw):
        dataframe = ctx.namespace[self.tablename]
        product = TableResolver(self.tablename,
                        autoincrement_col=self.autoincrement_col)
        df = product.resolve_dataframe(ctx)
        if self.whereclause is not None:
            df_ind = df[self.whereclause.resolve_expression(ctx, product)]
        else:
            df_ind = df

        # doing an UPDATE huh?  Yeah, this is quite slow, sorry.
        for ind in df_ind.index:
            product = DerivedResolver(df_ind.ix[ind:ind])

            for k, v in self.values:
                thing = v.resolve_expression(ctx, product)
                thing = _coerce_to_scalar(ctx, thing)

                dataframe[k][ind] = thing
        ctx.cursor.rowcount = len(df_ind)

class DeleteResolver(CRUDResolver):
    whereclause = None

    def __init__(self, tablename, autoincrement_col):
        self.tablename = tablename
        self.autoincrement_col = autoincrement_col

    def resolve(self, ctx, **kw):
        dataframe = ctx.namespace[self.tablename]
        product = TableResolver(self.tablename,
                        autoincrement_col=self.autoincrement_col)
        df = product.resolve_dataframe(ctx)
        if self.whereclause is not None:
            df_ind = df[self.whereclause.resolve_expression(ctx, product)]
        else:
            df_ind = df

        ctx.namespace[self.tablename] = dataframe.drop(df_ind.index)
        ctx.cursor.rowcount = len(df_ind)

class DDLResolver(Resolver):
    pass

class CreateTableResolver(DDLResolver):
    def __init__(self, tablename, colnames, coltypes, autoincrement_col, pandas_index_pk):
        self.tablename = tablename
        self.colnames = colnames
        self.coltypes = coltypes
        self.autoincrement_col = autoincrement_col
        self.pandas_index_pk = pandas_index_pk

    def resolve(self, ctx, **kw):
        if self.tablename in ctx.namespace:
            raise dbapi.Error("Dataframe '%s' already exists" % self.tablename)

        # TODO: this is a hack for now
        def get_type(type_):
            if isinstance(type_, sqltypes.Integer):
                return np.dtype('int64')
            elif isinstance(type_, sqltypes.Float):
                return np.dtype('float64')
            else:
                return np.dtype('object')

        ctx.namespace[self.tablename] = pd.DataFrame.from_items([
                (c, pd.Series(dtype=get_type(typ)))
                for (c, typ) in zip(self.colnames, self.coltypes)
                if not self.pandas_index_pk
                        or c != self.autoincrement_col
            ])

class DropTableResolver(DDLResolver):
    def __init__(self, tablename):
        self.tablename = tablename

    def resolve(self, ctx, **kw):
        if self.tablename not in ctx.namespace:
            raise dbapi.Error("No such dataframe '%s'" % self.tablename)
        del ctx.namespace[self.tablename]

def _coerce_to_series(ctx, col):
    if not isinstance(col, pd.Series):
        col = pd.Series([col])
    return col

def _coerce_to_scalar(ctx, col):
    if isinstance(col, pd.Series):
        col = col.reset_index(drop=True)
        if len(col) > 1:
            raise dbapi.Error("scalar expression "
                    "returned more than one row")
        col = col[0] if col else None
    return col

def _unique_name():
    names = collections.defaultdict(int)
    def go(name):
        count = names[name]
        names[name] += 1
        if count:
            return "%s_%d" % (name, count)
        else:
            return name
    return go

def _cartesian(ctx, f1, f2):
    """produce a cartesian product.

    This is to support multiple FROM clauses against a WHERE.

    Clearly, this is a bad place to be, and a join() should be
    used instead.   But this allows the results to come back,
    at least.

    """
    df1, df2 = f1.resolve_dataframe(ctx), f2.resolve_dataframe(ctx)

    return DerivedResolver(
                _cartesian_dataframe(ctx, df1, df2)
            )


def _cartesian_dataframe(ctx, df1, df2):
    if '_cartesian_ones' not in df1:
        df1['_cartesian_ones'] = np.ones(len(df1))
    if '_cartesian_ones' not in df2:
        df2['_cartesian_ones'] = np.ones(len(df2))
    return df1.merge(df2, on='_cartesian_ones')


"""Represent SQL tokens as Pandas operations.

"""
from sqlalchemy.sql import operators
from sqlalchemy import sql
from sqlalchemy import util
import functools
import pandas as pd
import collections

class Resolver(object):
    pass

class ColumnElementResolver(Resolver):
    def resolve_expression(self, api, product, namespace, params):
        raise NotImplementedError()

class FunctionResolver(ColumnElementResolver):
    def __init__(self, fn, expr, aggregate):
        self.fn = fn
        self.expr = expr
        self.aggregate = aggregate

    def resolve_expression(self, api, product, namespace, params):
        q = self.fn(self.expr.resolve_expression(
                    api, product, namespace, params))
        if self.aggregate:
            q = Aggregate(q)
        return q

class Aggregate(object):
    def __init__(self, value):
        self.value = value

class ColumnResolver(ColumnElementResolver):
    def __init__(self, name, tablename):
        self.name = name
        self.tablename = tablename

    def resolve_expression(self, api, product, namespace, params):
        if product is None:
            df = TableResolver(self.tablename).\
                        resolve_dataframe(api, namespace, params)
        else:
            df = product.resolve_dataframe(api, namespace, params)
        return api.df_getitem(df, self.df_index)

    @property
    def df_index(self):
        return "%s_%s" % (self.tablename, self.name)

class UnaryResolver(ColumnElementResolver):
    def __init__(self, expression, operator, modifier):
        self.operator = operator
        self.modifier = modifier
        self.expression = expression

    def resolve_expression(self, api, product, namespace, params):
        return self.expression.resolve_expression(api, product, namespace, params)

    @property
    def df_index(self):
        return self.expression.df_index

class LabelResolver(Resolver):
    def __init__(self, expression, name):
        self.expression = expression
        self.name = name

    def resolve_expression(self, api, product, namespace, params):
        return self.expression.resolve_expression(
                                        api,
                                        product, namespace, params)

    @property
    def df_index(self):
        return self.name

class FromResolver(Resolver):
    pass

class DerivedResolver(FromResolver):
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def resolve_dataframe(self, api, namespace, params, names=True):
        return self.dataframe


class TableResolver(FromResolver):
    def __init__(self, tablename):
        self.tablename = tablename

    def resolve_dataframe(self, api, namespace, params, names=True):
        df = namespace[self.tablename]
        if names:
            df = api.dataframe(
                        dict(
                            ("%s_%s" % (self.tablename, k), df[k])
                            for k in df.keys()
                        ))
        return df

class JoinResolver(FromResolver):
    def __init__(self, left, right, onclause, isouter):
        self.left = left
        self.right = right
        self.onclause = onclause
        self.isouter = isouter

    def resolve_dataframe(self, api, namespace, params, names=True):
        df1 = left = self.left.resolve_dataframe(api, namespace, params)
        df2 = self.right.resolve_dataframe(api, namespace, params)

        if self.isouter:
            left['_cp_left_index'] = left.index

        straight_binaries, remainder = self._produce_join_expressions(df1, df2)

        df1 = self._merge_straight_binaries(api, df1, df2, straight_binaries)

        df1 = self._merge_remainder(api, left, df1, df2,
                            namespace, params, straight_binaries, remainder)
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

    def _merge_straight_binaries(self, api, df1, df2, straight_binaries):
        if straight_binaries:
            # use merge() for straight binaries.
            left_on, right_on = zip(*straight_binaries)
            df1 = api.merge(df1, df2, left_on=left_on, right_on=right_on,
                            how='left' if self.isouter else 'inner')
        return df1

    def _merge_remainder(self, api, left, df1, df2,
                            namespace, params, straight_binaries, remainder):
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
                df1 = _cartesian_dataframe(api, df1, df2)
            expr = remainder.resolve_expression(
                        api, DerivedResolver(df1), namespace, params
                    )

            joined = api.df_getitem(df1, expr)

            if self.isouter:
                # for outer join, grab remaining rows from "left"
                remaining_left_ids = set(df1['_cp_left_index']).\
                                        difference(joined['_cp_left_index'])
                remaining = left.ix[remaining_left_ids]
                df1 = pd.concat([joined, remaining]).reset_index()
            else:
                df1 = joined
        return df1


class AliasResolver(FromResolver):
    def __init__(self, table, aliasname):
        self.table = table
        self.aliasname = aliasname

    def resolve_dataframe(self, api, namespace, params, names=True):
        df = self.table.resolve_dataframe(api, namespace, params, names=False)
        if names:
            df = api.dataframe(
                        dict(
                            ("%s_%s" % (self.aliasname, k), df[k])
                            for k in df.keys()
                        ))
        return df

class BinaryResolver(ColumnElementResolver):
    def __init__(self, left, right, operator):
        self.left = left
        self.right = right
        self.operator = operator

    def resolve_expression(self, api, product, namespace, params):
        return self.operator(
                    self.left.resolve_expression(
                                            api, product, namespace, params),
                    self.right.resolve_expression(
                                            api, product, namespace, params),
                )

class ClauseListResolver(ColumnElementResolver):
    def __init__(self, expressions, operator):
        self.expressions = expressions
        self.operator = operator

    def resolve_expression(self, api, product, namespace, params):
        exprs = [expr.resolve_expression(
                                            api,
                                            product, namespace, params)
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

    def resolve_expression(self, api, product, namespace, params):
        return params[self.name]

class BaseSelectResolver(FromResolver):
    group_by = None
    order_by = None
    limit = None
    offset = None

    @util.memoized_property
    def columns(self):
        return []

    def _evaluate(self, api, namespace, params, correlate=None):
        raise NotImplementedError()

    def __call__(self, api, namespace, params, correlate=None):
        product = self._evaluate(api, namespace, params, correlate)

        if self.group_by is not None:
            df = product.resolve_dataframe(api, namespace, params)
            gp = self.group_by.resolve_expression(
                            api, product, namespace, params)
            groups = [DerivedResolver(gdf[1]) for gdf in df.groupby(gp)]
        else:
            groups = [product]


        def process_aggregates(gprod):
            """detect aggregate funcitons in column clauses and
            flatten results if present
            """
            cols = [c.resolve_expression(api, gprod, namespace,
                                                    params)
                    for c in self.columns]
            for c in cols:
                if isinstance(c, Aggregate):
                    break
            else:
                return cols

            return [
                list(c)[0]
                    if not isinstance(c, Aggregate)
                    else [c.value]
                for c in cols
            ]

        nu = unique_name()
        names = [nu(c.name) for c in self.columns]

        #results = api.concat([api.df_from_items([process_aggregates(gprod)]) for gprod in groups])

        group_results = [
            api.df_from_items(
                    [
                        (
                            c.df_index,
                            expr
                        )
                        for c, expr in zip(self.columns, process_aggregates(gprod))
                    ])
            for gprod in groups
        ]
        results = api.concat(group_results)

        if self.order_by:
            cols = []
            asc = []
            for idx, ob_expr in enumerate(self.order_by.expressions):
                ascending = \
                    not isinstance(ob_expr, UnaryResolver) or \
                    ob_expr.modifier is not operators.desc_op
                key = '_sort_%d' % idx
                results[key] = ob_expr.resolve_expression(
                        api, DerivedResolver(results), namespace, params)
                cols.append(key)
                asc.append(ascending)
            results = results.sort(columns=cols, ascending=asc).\
                            reset_index(drop=True)
            for col in cols:
                del results[col]

        api.rename(results, columns=dict(
                        (col.df_index, name)
                        for col, name in zip(self.columns, names)
                        ), inplace=True)

        return results

class SelectResolver(BaseSelectResolver):
    whereclause = None

    @util.memoized_property
    def dataframes(self):
        return []


    def resolve_dataframe(self, api, namespace, params, names=True):
        return self(api, namespace, params)

    def resolve_expression(self, api, product, namespace, params):
        # correlated subquery - resolve for every row.
        # TODO: probably *dont* need to resolve for every row
        # for an uncorrelated subquery, can detect that
        p_df = product.resolve_dataframe(api, namespace, params)

        # iterate through rows in dataframe and form one-row
        # dataframes.  The ind:ind thing is the only way I could
        # figure out to achieve this, might be an eaiser way.
        things = []
        for ind in api.df_index(p_df):
            row = api.df_ix_getitem(p_df, slice(ind, ind))
            df = DerivedResolver(row)
            thing = self._evaluate(api, namespace, params, correlate=df)

            if len(thing) > 1:
                raise Exception("Subquery returned more than one row")

            # return as a simple list of scalar values.
            # the None is for those rows which we had no value
            things.append(thing[0] if thing else None)
        return things

    def _evaluate(self, api, namespace, params, correlate=None):
        if not self.dataframes:
            # "null" dataframe
            product = DerivedResolver(api.dataframe(
                        [{col.df_index: [1]} for col in self.columns]))
        else:
            product = self.dataframes[0]
        for df in self.dataframes[1:]:
            product = _cartesian(api, product, df, namespace, params)
        if correlate:
            product = _cartesian(api, product, correlate, namespace, params)
        df = product.resolve_dataframe(api, namespace, params)
        if self.whereclause is not None:
            df = api.df_getitem(df, self.whereclause.resolve_expression(
                            api,
                            product, namespace, params))

        product = DerivedResolver(df)
        if correlate:
            col = self.columns[0].resolve_expression(
                            api,
                            product, namespace, params)
            return api.reset_index(col, drop=True)
        return product


class CompoundResolver(BaseSelectResolver):
    keyword = None

    @util.memoized_property
    def selects(self):
        return []

    def resolve_dataframe(self, api, namespace, params, names=True):
        return self(api, namespace, params)

    def _evaluate(self, api, namespace, params, correlate=None, **kw):
        assert self.keyword is sql.CompoundSelect.UNION_ALL

        evaluated = [
            sel(api, namespace, params, **kw)
            for sel in self.selects
        ]

        self.columns = self.selects[0].columns

        for ev in evaluated:
            api.rename(ev, columns=dict(
                    (old, new.df_index) for old, new in
                    zip(ev.keys(), self.columns)
                ),
                inplace=True)

        df = api.concat(evaluated)
        return DerivedResolver(df)


def unique_name():
    names = collections.defaultdict(int)
    def go(name):
        count = names[name]
        names[name] += 1
        if count:
            return "%s_%d" % (name, count)
        else:
            return name
    return go

def _cartesian(api, f1, f2, namespace, params):
    """produce a cartesian product.

    This is to support multiple FROM clauses against a WHERE.

    Clearly, this is a bad place to be, and a join() should be
    used instead.   But this allows the results to come back,
    at least.

    """
    df1, df2 = f1.resolve_dataframe(api, namespace, params), \
                    f2.resolve_dataframe(api, namespace, params)

    return DerivedResolver(
                _cartesian_dataframe(api, df1, df2)
            )


def _cartesian_dataframe(api, df1, df2):
    if '_cartesian_ones' not in df1:
        df1['_cartesian_ones'] = api.np_ones(len(df1))
    if '_cartesian_ones' not in df2:
        df2['_cartesian_ones'] = api.np_ones(len(df2))
    return api.merge(df1, df2, on='_cartesian_ones')


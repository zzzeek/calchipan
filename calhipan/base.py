from sqlalchemy.sql import compiler
from sqlalchemy import exc
from sqlalchemy.sql import expression as sql, operators
from sqlalchemy import util
from sqlalchemy.engine import default
import functools


class PandasCompiler(compiler.SQLCompiler):
    def __init__(self, *arg, **kw):
        super(PandasCompiler, self).__init__(*arg, **kw)
        self._panda_fn = self.string
        self.string = "placeholder"

    def visit_column(self, column, add_to_result_map=None,
                                    include_table=True, **kwargs):

        name = orig_name = column.name
        if name is None:
            raise exc.CompileError("Cannot compile Column object until "
                                   "its 'name' is assigned.")

        is_literal = column.is_literal
        if not is_literal and isinstance(name, sql._truncated_label):
            name = self._truncated_identifier("colident", name)

        if add_to_result_map is not None:
            add_to_result_map(
                name,
                orig_name,
                (column, name, column.key),
                column.type
            )

        tablename = None
        table = column.table
        if table is not None and include_table and table.named_with_column:
            tablename = table.name
            if isinstance(tablename, sql._truncated_label):
                tablename = self._truncated_identifier("alias", tablename)

        return ColumnAdapter(name, tablename)

    def visit_label(self, label,
                            add_to_result_map=None,
                            within_label_clause=False,
                            within_columns_clause=False, **kw):
        # only render labels within the columns clause
        # or ORDER BY clause of a select.  dialect-specific compilers
        # can modify this behavior.
        if within_columns_clause and not within_label_clause:
            if isinstance(label.name, sql._truncated_label):
                labelname = self._truncated_identifier("colident", label.name)
            else:
                labelname = label.name

            if add_to_result_map is not None:
                add_to_result_map(
                        labelname,
                        label.name,
                        (label, labelname, ) + label._alt_names,
                        label.type
                )

            return LabelAdapter(
                        label.element._compiler_dispatch(self,
                                    within_columns_clause=True,
                                    within_label_clause=True,
                                    **kw),
                        labelname
                    )
        else:
            return label.element._compiler_dispatch(self,
                                    within_columns_clause=False,
                                    **kw)

    def visit_concat_op_binary(self, binary, operator, **kw):
        kw['override_op'] = operators.add
        return self.visit_binary(binary, **kw)

    def visit_clauselist(self, clauselist, **kwargs):
        return ClauseListAdapter(
                    [s for s in
                    (c._compiler_dispatch(self, **kwargs)
                    for c in clauselist.clauses)
                    if s], clauselist.operator)

    def visit_table(self, table, asfrom=False, iscrud=False, ashint=False,
                        fromhints=None, **kwargs):
        return TableAdapter(table.name)

    def visit_grouping(self, grouping, asfrom=False, **kwargs):
        return grouping.element._compiler_dispatch(self, **kwargs)

    def visit_alias(self, alias, asfrom=False, ashint=False,
                                iscrud=False,
                                fromhints=None, **kwargs):
        if asfrom or ashint:
            if isinstance(alias.name, sql._truncated_label):
                alias_name = self._truncated_identifier("alias", alias.name)
            else:
                alias_name = alias.name

        if asfrom:
            return AliasAdapter(
                        alias.original._compiler_dispatch(self,
                                asfrom=True, **kwargs),
                        alias_name
                    )
        else:
            return alias.original._compiler_dispatch(self, **kwargs)

    def visit_join(self, join, asfrom=False, **kwargs):
        return JoinAdapter(
                    join.left._compiler_dispatch(self, asfrom=True, **kwargs),
                    join.right._compiler_dispatch(self, asfrom=True, **kwargs),
                    join.onclause._compiler_dispatch(self, **kwargs),
                    join.isouter
                )

    def visit_binary(self, binary, override_op=None, **kw):
        operator = override_op or binary.operator

        disp = getattr(self, "visit_%s_binary" % operator.__name__, None)
        if disp:
            return disp(binary, operator, **kw)
        else:
            return BinaryAdapter(
                        binary.left._compiler_dispatch(self, **kw),
                        binary.right._compiler_dispatch(self, **kw),
                        operator
                    )


    def bindparam_string(self, name, **kw):
        return BindParamAdapter(name)

    def visit_select(self, select, asfrom=False, parens=True,
                            iswrapper=False, fromhints=None,
                            compound_index=0,
                            force_result_map=False,
                            positional_names=None, **kwargs):

        entry = self.stack and self.stack[-1] or {}

        existingfroms = entry.get('from', None)

        froms = select._get_display_froms(existingfroms, asfrom=asfrom)

        correlate_froms = set(sql._from_objects(*froms))

        # TODO: might want to propagate existing froms for
        # select(select(select)) where innermost select should correlate
        # to outermost if existingfroms: correlate_froms =
        # correlate_froms.union(existingfroms)

        populate_result_map = force_result_map or (
                                compound_index == 0 and (
                                    not entry or \
                                    entry.get('iswrapper', False)
                                )
                            )

        self.stack.append({'from': correlate_froms,
                            'iswrapper': iswrapper})

        sel = SelectAdapter()

        column_clause_args = kwargs.copy()
        column_clause_args.update({
                'positional_names': positional_names,
                'within_label_clause': False,
                'within_columns_clause': False
            })

        # the actual list of columns to print in the SELECT column list.
        inner_columns = [
            c for c in [
                self._label_select_column(select, column,
                                    populate_result_map, asfrom,
                                    column_clause_args)
                for column in util.unique_list(select.inner_columns)
                ]
            if c is not None
        ]
        sel.columns.extend(inner_columns)

        if froms:
            for f in froms:
                sel.dataframes.append(
                    f._compiler_dispatch(self, asfrom=True, **kwargs)
                )

        if select._whereclause is not None:
            t = select._whereclause._compiler_dispatch(self, **kwargs)
            sel.whereclause = t

        self.stack.pop(-1)

        return sel

class PandasDialect(default.DefaultDialect):
    statement_compiler = PandasCompiler

class Adapter(object):
    pass

class ColumnElementAdapter(Adapter):
    def resolve_expression(self, trace, product, namespace, params):
        raise NotImplementedError()

class ColumnAdapter(ColumnElementAdapter):
    def __init__(self, name, tablename):
        self.name = name
        self.tablename = tablename

    def resolve_expression(self, trace, product, namespace, params):
        if product is None:
            df = TableAdapter(self.tablename).\
                        resolve_dataframe(trace, namespace, params)
        else:
            df = product.resolve_dataframe(trace, namespace, params)
        return trace.df_getitem(df, self.df_index)

    @property
    def df_index(self):
        return "%s_cp_%s" % (self.name, self.tablename)

class LabelAdapter(Adapter):
    def __init__(self, expression, name):
        self.expression = expression
        self.name = name

    def resolve_expression(self, trace, product, namespace, params):
        return self.expression.resolve_expression(
                                        trace,
                                        product, namespace, params)

    @property
    def df_index(self):
        return self.name

class FromAdapter(Adapter):
    pass

class DerivedAdapter(FromAdapter):
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def resolve_dataframe(self, trace, namespace, params, names=True):
        return self.dataframe


class TableAdapter(FromAdapter):
    def __init__(self, tablename):
        self.tablename = tablename

    def resolve_dataframe(self, trace, namespace, params, names=True):
        df = namespace[self.tablename]
        if names:
            df = trace.dataframe(
                        dict(
                            ("%s_cp_%s" % (k, self.tablename), df[k])
                            for k in df.keys()
                        ))
        return df



class JoinAdapter(FromAdapter):
    def __init__(self, left, right, onclause, isouter):
        self.left = left
        self.right = right
        self.onclause = onclause
        self.isouter = isouter

    def resolve_dataframe(self, trace, namespace, params, names=True):
        df1, df2 = self.left.resolve_dataframe(trace, namespace, params), \
                        self.right.resolve_dataframe(trace, namespace, params)

        straight_binaries = []
        remainder = []
        if isinstance(self.onclause, ClauseListAdapter) and \
                self.onclause.operator is operators.and_:
            comparisons = self.onclause.expressions
        else:
            comparisons = [self.onclause]

        # extract comparisons like this:
        # col1 == col2 AND col3 == col4 AND ...
        # use pd.merge() for those
        for comp in comparisons:
            if isinstance(comp, BinaryAdapter) and \
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

        if straight_binaries:
            left_on, right_on = zip(*straight_binaries)
            df1 = trace.merge(df1, df2, left_on=left_on, right_on=right_on)

        # for everything else, use cartesian product
        # plus expressions
        if remainder:
            if len(remainder) > 1:
                remainder = ClauseListAdapter(remainder, operators.and_)
            else:
                remainder = remainder[0]

            # if we haven't joined them together yet,
            # do a cartesian.... is this a little bit like,
            # "things are more efficient if we happened to join on the index"?
            if not straight_binaries:
                df1 = _cartesian_dataframe(trace, df1, df2)
            df1 = trace.df_getitem(df1, remainder.resolve_expression(trace,
                    DerivedAdapter(df1), namespace, params))
        return df1



class AliasAdapter(FromAdapter):
    def __init__(self, table, aliasname):
        self.table = table
        self.aliasname = aliasname

    def resolve_dataframe(self, trace, namespace, params, names=True):
        df = self.table.resolve_dataframe(trace, namespace, params, names=False)
        if names:
            df = trace.dataframe(
                        dict(
                            ("%s_cp_%s" % (k, self.aliasname), df[k])
                            for k in df.keys()
                        ))
        return df

class BinaryAdapter(ColumnElementAdapter):
    def __init__(self, left, right, operator):
        self.left = left
        self.right = right
        self.operator = operator

    def resolve_expression(self, trace, product, namespace, params):
        return self.operator(
                    self.left.resolve_expression(trace, product, namespace, params),
                    self.right.resolve_expression(trace, product, namespace, params),
                )

class ClauseListAdapter(ColumnElementAdapter):
    def __init__(self, expressions, operator):
        self.expressions = expressions
        self.operator = operator

    def resolve_expression(self, trace, product, namespace, params):
        return functools.reduce(
                    self.operator,
                    [
                        expr.resolve_expression(
                                            trace,
                                            product, namespace, params)
                        for expr in self.expressions
                    ]
                )

class BindParamAdapter(ColumnElementAdapter):
    def __init__(self, name):
        self.name = name

    def resolve_expression(self, trace, product, namespace, params):
        return params[self.name]

class SelectAdapter(FromAdapter):
    whereclause = None

    @util.memoized_property
    def dataframes(self):
        return []

    @util.memoized_property
    def columns(self):
        return []

    def resolve_dataframe(self, trace, namespace, params, names=True):
        return self(trace, namespace, params)

    def resolve_expression(self, trace, product, namespace, params):
        # correlated subquery - resolve for every row.
        # TODO: probably *dont* need to resolve for every row
        # for an uncorrelated subquery, can detect that
        p_df = product.resolve_dataframe(trace, namespace, params)

        # iterate through rows in dataframe and form one-row
        # dataframes.  The ind:ind thing is the only way I could
        # figure out to achieve this, might be an eaiser way.
        things = []
        for ind in trace.df_index(p_df):
            row = trace.df_ix_getitem(p_df, slice(ind, ind))
            df = DerivedAdapter(row)
            thing = self(trace, namespace, params, correlate=df)

            # return as a simple list of scalar values.
            # the None is for those rows which we had no value
            things.append(thing[0] if thing else None)
        return things

    def __call__(self, trace, namespace, params, correlate=None):
        product = self.dataframes[0]
        for df in self.dataframes[1:]:
            product = _cartesian(trace, product, df, namespace, params)
        if correlate:
            product = _cartesian(trace, product, correlate, namespace, params)
        df = product.resolve_dataframe(trace, namespace, params)
        if self.whereclause is not None:
            df = trace.df_getitem(df, self.whereclause.resolve_expression(
                            trace,
                            product, namespace, params))

        product = DerivedAdapter(df)
        if correlate:
            col = self.columns[0].resolve_expression(
                            trace,
                            product, namespace, params)
            return trace.reset_index(col, drop=True)
        nu = unique_name()
        return trace.df_from_items(
                    [
                        (
                            nu(c.name),
                            c.resolve_expression(trace, product, namespace,
                                                    params)
                        )
                        for c in self.columns
                    ])

import collections
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

def _cartesian(trace, f1, f2, namespace, params):
    """produce a cartesian product.

    This is to support multiple FROM clauses against a WHERE.

    Clearly, this is a bad place to be, and a join() should be
    used instead.   But this allows the results to come back,
    at least.

    """
    df1, df2 = f1.resolve_dataframe(trace, namespace, params), \
                    f2.resolve_dataframe(trace, namespace, params)

    return DerivedAdapter(
                _cartesian_dataframe(trace, df1, df2)
            )


def _cartesian_dataframe(trace, df1, df2):
    if '_calhipan_ones' not in df1:
        df1['_calhipan_ones'] = trace.np_ones(len(df1))
    if '_calhipan_ones' not in df2:
        df2['_calhipan_ones'] = trace.np_ones(len(df2))
    return trace.merge(df1, df2, on='_calhipan_ones')


from sqlalchemy.sql import compiler
from sqlalchemy import exc
from sqlalchemy.sql import expression as sql
from sqlalchemy import util
from sqlalchemy.engine import default
import numpy as np
import pandas as pd

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
        if table is not None and include_table and table.named_with_column \
                and not isinstance(table, sql.TableClause):
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

    def visit_table(self, table, asfrom=False, iscrud=False, ashint=False,
                        fromhints=None, **kwargs):
        return TableAdapter(table.name)

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

    def visit_binary(self, binary, **kw):
        operator = binary.operator

        # TODO: special dispatch
        #disp = getattr(self, "visit_%s_binary" % operator.__name__, None)
        #if disp:
        #    return disp(binary, operator, **kw)
        #else:
        #    return self._generate_generic_binary(binary,
        #                        OPERATORS[operator], **kw)

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
    def resolve_expression(self, df, params):
        raise NotImplementedError()

class ColumnAdapter(ColumnElementAdapter):
    def __init__(self, name, tablename):
        self.name = name
        self.tablename = tablename

    def resolve_expression(self, df, params):
        return df[self.df_index]

    @property
    def df_index(self):
        if self.tablename is None:
            return self.name
        else:
            return "%s_cp_%s" % (self.name, self.tablename)

class LabelAdapter(Adapter):
    def __init__(self, expression, name):
        self.expression = expression
        self.name = name

    def resolve_expression(self, df, params):
        return self.expression.resolve_expression(df, params)

    @property
    def df_index(self):
        return self.name

class FromAdapter(Adapter):
    pass

class DerivedAdapter(FromAdapter):
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def resolve_dataframe(self, namespace, params):
        return self.dataframe

    @property
    def suffix(self):
        return "_cp_%d" % id(self)

class TableAdapter(FromAdapter):
    def __init__(self, tablename):
        self.tablename = tablename

    def resolve_dataframe(self, namespace, params):
        return namespace[self.tablename]

    @property
    def suffix(self):
        return "_cp_%s" % self.tablename

class AliasAdapter(FromAdapter):
    def __init__(self, table, aliasname):
        self.table = table
        self.aliasname = aliasname

    def resolve_dataframe(self, namespace, params):
        df = self.table.resolve_dataframe(namespace, params)
        return pd.DataFrame(
                        dict(
                            ("%s_cp_%s" % (k, self.aliasname), df[k])
                            for k in df.keys()
                        ))

    @property
    def suffix(self):
        return "_cp_%s" % self.aliasname

class BinaryAdapter(ColumnElementAdapter):
    def __init__(self, left, right, operator):
        self.left = left
        self.right = right
        self.operator = operator

    def resolve_expression(self, df, params):
        return self.operator(
                    self.left.resolve_expression(df, params),
                    self.right.resolve_expression(df, params),
                )

class BindParamAdapter(ColumnElementAdapter):
    def __init__(self, name):
        self.name = name

    def resolve_expression(self, namespace, params):
        return params[self.name]

class SelectAdapter(FromAdapter):
    whereclause = None

    @util.memoized_property
    def dataframes(self):
        return []

    @util.memoized_property
    def columns(self):
        return []

    def __call__(self, namespace, params):
        product = self.dataframes[0]
        for df in self.dataframes[1:]:
            product = _cartesian(product, df, namespace, params)
        df = product.resolve_dataframe(namespace, params)
        if self.whereclause is not None:
            df = df[self.whereclause.resolve_expression(df, params)]
        return pd.DataFrame(
                    dict(
                        (c.df_index, c.resolve_expression(df, params))
                        for c in self.columns))

def _cartesian(f1, f2, namespace, params):
    """produce a cartesian product.

    This is to support multiple FROM clauses against a WHERE.

    Clearly, this is a bad place to be, and a join() should be
    used instead.   But this allows the results to come back,
    at least.

    """
    df1, df2 = f1.resolve_dataframe(namespace, params), \
                    f2.resolve_dataframe(namespace, params)

    if '_calhipan_ones' not in df1:
        df1['_calhipan_ones'] = np.ones(len(df1))
    if '_calhipan_ones' not in df2:
        df2['_calhipan_ones'] = np.ones(len(df2))

    return DerivedAdapter(pd.merge(df1, df2, on='_calhipan_ones',
                suffixes=(f1.suffix, f2.suffix)))


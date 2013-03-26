"""SQLAlchemy Compiler implementation.

This compiler, instead of returning SQL strings, returns
a "resolver" structure that can invoke specific commands to the
Pandas API.

"""
from sqlalchemy.sql import compiler
from sqlalchemy import exc
from sqlalchemy.sql import expression as sql, operators
from sqlalchemy import util
from . import resolver
import datetime
import pandas as pd
from . import operators as ca_operators

class PandasDDLCompiler(compiler.DDLCompiler):
    statement = None

    def __init__(self, *arg, **kw):
        super(PandasDDLCompiler, self).__init__(*arg, **kw)
        if self.statement is not None:
            self._panda_fn = self.string
            self.string = str(self.string)

    def visit_create_table(self, create, **kw):
        table = create.element
        return resolver.CreateTableResolver(table.name,
                    [c.name for c in table.c],
                    [c.type for c in table.c],
                    table._autoincrement_column.name
                    if table._autoincrement_column is not None else None,
                    table.kwargs.get('pandas_index_pk', False))

    def visit_create_view(self, view, **kw):
        raise NotImplementedError()

    def visit_drop_view(self, view, **kw):
        raise NotImplementedError()

    def visit_drop_table(self, drop, **kw):
        table = drop.element
        return resolver.DropTableResolver(table.name)

    def visit_create_index(self, ind, **kw):
        return resolver.NullResolver()

    def visit_drop_index(self, ind, **kw):
        return resolver.NullResolver()

    def visit_create_constraint(self, ind, **kw):
        return resolver.NullResolver()

    def visit_drop_constraint(self, ind, **kw):
        return resolver.NullResolver()

class PandasCompiler(compiler.SQLCompiler):
    statement = None

    def __init__(self, *arg, **kw):
        super(PandasCompiler, self).__init__(*arg, **kw)
        if self.statement is not None:
            self._panda_fn = self.string
            self.string = str(self.string)

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

        if is_literal:
            assert tablename is None
            assert table is None
            return resolver.LiteralResolver(name)
        else:
            return resolver.ColumnResolver(name, tablename)

    def visit_unary(self, unary, **kw):
        return resolver.UnaryResolver(
                    unary.element._compiler_dispatch(self, **kw),
                    modifier=unary.modifier,
                    operator=unary.operator)

    def visit_function(self, func, add_to_result_map=None, **kwargs):
        # this is only re-implemented so that we can raise
        # on functions not implemented right here

        if add_to_result_map is not None:
            add_to_result_map(
                func.name, func.name, (), func.type
            )

        disp = getattr(self, "visit_%s_func" % func.name.lower(), None)
        if disp:
            return disp(func, **kwargs)
        elif hasattr(func, "pandas_fn"):
            if getattr(func, "pandas_aggregate", False):
                return self._aggregate_on(func, func.pandas_fn, **kwargs)
            else:
                return self._scalar_fn_on(func, func.pandas_fn, **kwargs)

        else:
            raise exc.CompileError(
                    "Pandas dialect has no '%s()' function implemented" %
                    func.name.lower())

    def visit_label(self, label,
                            add_to_result_map=None,
                            within_label_clause=False,
                            within_columns_clause=False, **kw):
        # only resolver labels within the columns clause
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

            return resolver.LabelResolver(
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

    def visit_in_op_binary(self, binary, operator, **kw):
        kw['override_op'] = ca_operators.in_op
        return self.visit_binary(binary, **kw)

    def visit_is__binary(self, binary, operator, **kw):
        kw['override_op'] = ca_operators.is_op
        return self.visit_binary(binary, **kw)

    def visit_isnot_binary(self, binary, operator, **kw):
        kw['override_op'] = ca_operators.isnot_op
        return self.visit_binary(binary, **kw)

    def _aggregate_on(self, func, fn, **kw):
        return resolver.FunctionResolver(
                    fn,
                    func.clause_expr._compiler_dispatch(self, **kw),
                    True
                )

    def _scalar_fn_on(self, func, fn, **kw):
        return resolver.FunctionResolver(
                    fn,
                    func.clause_expr._compiler_dispatch(self, **kw),
                    False
                )

    def visit_now_func(self, func, **kw):
        return self._scalar_fn_on(func,
                    lambda arg: pd.Series([datetime.datetime.now()]), **kw)

    def visit_count_func(self, func, **kw):
        return self._aggregate_on(func, len, **kw)

    def visit_max_func(self, func, **kw):
        return self._aggregate_on(func, max, **kw)

    def visit_min_func(self, func, **kw):
        return self._aggregate_on(func, min, **kw)

    def visit_null(self, expr, **kw):
        return resolver.ConstantResolver(None)

    def visit_true(self, expr, **kw):
        return resolver.ConstantResolver(True)

    def visit_false(self, expr, **kw):
        return resolver.ConstantResolver(False)

    def visit_clauselist(self, clauselist, **kwargs):
        return resolver.ClauseListResolver(
                    [s for s in
                    (c._compiler_dispatch(self, **kwargs)
                    for c in clauselist.clauses)
                    if s], clauselist.operator)

    def visit_table(self, table, asfrom=False, iscrud=False, ashint=False,
                        fromhints=None, **kwargs):
        autoinc_col = table._autoincrement_column
        return resolver.TableResolver(table.name,
                    autoinc_col.name if autoinc_col is not None else None)

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
            return resolver.AliasResolver(
                        alias.original._compiler_dispatch(self,
                                asfrom=True, **kwargs),
                        alias_name
                    )
        else:
            return alias.original._compiler_dispatch(self, **kwargs)

    def visit_join(self, join, asfrom=False, **kwargs):
        return resolver.JoinResolver(
                    join.left._compiler_dispatch(self, asfrom=True, **kwargs),
                    join.right._compiler_dispatch(self, asfrom=True, **kwargs),
                    join.onclause._compiler_dispatch(self, **kwargs),
                    join.isouter
                )

    def visit_binary(self, binary, override_op=None, **kw):
        operator = override_op or binary.operator

        if override_op is None:
            disp = getattr(self, "visit_%s_binary" % operator.__name__, None)
            if disp:
                return disp(binary, operator, **kw)

        return resolver.BinaryResolver(
                    binary.left._compiler_dispatch(self, **kw),
                    binary.right._compiler_dispatch(self, **kw),
                    operator
                )


    def bindparam_string(self, name, **kw):
        return resolver.BindParamResolver(name)

    def order_by_clause(self, select, **kw):
        order_by = select._order_by_clause._compiler_dispatch(self, **kw)
        return order_by

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

        sel = resolver.SelectResolver()

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

        if select._group_by_clause.clauses:
            group_by = select._group_by_clause._compiler_dispatch(
                                        self, **kwargs)
            sel.group_by = group_by

        if select._order_by_clause.clauses:
            order_by = select._order_by_clause._compiler_dispatch(
                                        self, **kwargs)
            sel.order_by = order_by

        if select._having is not None:
            sel.having = select._having._compiler_dispatch(self, **kwargs)

        sel.limit = select._limit
        sel.offset = select._offset

        self.stack.pop(-1)

        return sel

    def visit_compound_select(self, cs, asfrom=False,
                            parens=True, compound_index=0, **kwargs):
        entry = self.stack and self.stack[-1] or {}
        self.stack.append({'from': entry.get('from', None),
                    'iswrapper': not entry})

        compound = resolver.CompoundResolver()
        compound.keyword = cs.keyword

        for i, stmt in enumerate(cs.selects):
            compound.selects.append(
                stmt._compiler_dispatch(self,
                                            asfrom=asfrom, parens=False,
                                            compound_index=i, **kwargs)

            )

        if cs._group_by_clause.clauses:
            compound.group_by = cs._group_by_clause._compiler_dispatch(
                                self, asfrom=asfrom, **kwargs)
        if cs._order_by_clause.clauses:
            compound.order_by = cs._order_by_clause._compiler_dispatch(
                                self, **kwargs
                                )

        # compounds have GROUP BY but not HAVING?  seems like a bug
        #if cs._having is not None:
        #    compound.having = cs._having._compiler_dispatch(
        #                        self, **kwargs)

        compound.limit = cs._limit
        compound.offset = cs._offset

        self.stack.pop(-1)

        return compound

    def visit_insert(self, insert_stmt, **kw):
        self.isinsert = True
        colparams = self._get_colparams(insert_stmt)

        if insert_stmt._has_multi_parameters:
            colparams_single = colparams[0]
        else:
            colparams_single = colparams

        ins = resolver.InsertResolver(
                    insert_stmt.table.name,
                    insert_stmt.table.kwargs.get('pandas_index_pk', False)
                )

        ins.columns = [c[0].name for c in colparams_single]

        if self.returning or insert_stmt._returning:
            raise NotImplementedError("soon...")

        if insert_stmt._has_multi_parameters:
            ins.values = [[c[1] for c in colparam_set] for colparam_set in colparams]
        else:
            ins.values = [c[1] for c in colparams]

        return ins

    def visit_update(self, update_stmt, **kw):
        self.stack.append({'from': set([update_stmt.table])})

        self.isupdate = True

        extra_froms = update_stmt._extra_froms

        colparams = self._get_colparams(update_stmt, extra_froms)

        upd = resolver.UpdateResolver(update_stmt.table.name,
                            update_stmt.table._autoincrement_column.name
                            if update_stmt.table._autoincrement_column
                            is not None else None)

        upd.values = [
            (
                c[0].name,
                c[1]
            )  for c in colparams
        ]

        if update_stmt._returning:
            raise NotImplementedError("soon...")

        if extra_froms:
            raise NotImplementedError("multiple UPDATE froms not implemented")

        if update_stmt._whereclause is not None:
            upd.whereclause = self.process(update_stmt._whereclause)

        self.stack.pop(-1)

        return upd

    def visit_delete(self, delete_stmt, **kw):
        self.stack.append({'from': set([delete_stmt.table])})
        self.isdelete = True

        del_ = resolver.DeleteResolver(delete_stmt.table.name,
                            delete_stmt.table._autoincrement_column.name
                            if delete_stmt.table._autoincrement_column
                            is not None else None)

        if delete_stmt._returning:
            raise NotImplementedError("soon...")

        if delete_stmt._whereclause is not None:
            del_.whereclause = self.process(delete_stmt._whereclause)


        self.stack.pop(-1)

        return del_


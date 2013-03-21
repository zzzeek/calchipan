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

        return resolver.ColumnResolver(name, tablename)

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

    def _aggregate_on(self, func, fn, **kw):
        return resolver.FunctionResolver(
                    fn,
                    func.clause_expr._compiler_dispatch(self, **kw),
                    True
                )

    def visit_count_func(self, func, **kw):
        return self._aggregate_on(func, len, **kw)

    def visit_max_func(self, func, **kw):
        return self._aggregate_on(func, max, **kw)

    def visit_min_func(self, func, **kw):
        return self._aggregate_on(func, min, **kw)

    def visit_clauselist(self, clauselist, **kwargs):
        return resolver.ClauseListResolver(
                    [s for s in
                    (c._compiler_dispatch(self, **kwargs)
                    for c in clauselist.clauses)
                    if s], clauselist.operator)

    def visit_table(self, table, asfrom=False, iscrud=False, ashint=False,
                        fromhints=None, **kwargs):
        return resolver.TableResolver(table.name)

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

        disp = getattr(self, "visit_%s_binary" % operator.__name__, None)
        if disp:
            return disp(binary, operator, **kw)
        else:
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

        compound.group_by = cs._group_by_clause._compiler_dispatch(
                                self, asfrom=asfrom, **kwargs)
        compound.order_by = self.order_by_clause(cs, **kwargs)
        compound.limit = cs._limit
        compound.offset = cs._offset

        self.stack.pop(-1)

        return compound



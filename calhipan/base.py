from sqlalchemy.engine import default
from sqlalchemy import exc
from .compiler import PandasCompiler
from . import dbapi

class PandasExecutionContext(default.DefaultExecutionContext):
    pass


class PandasDialect(default.DefaultDialect):
    statement_compiler = PandasCompiler
    execution_ctx_cls = PandasExecutionContext

    def __init__(self, namespace=None, **kw):
        super(PandasDialect, self).__init__(**kw)
        self.namespace = namespace

    @classmethod
    def dbapi(cls):
        return dbapi

    def create_connect_args(self, url):
        return [], {"namespace": self.namespace}

    def initialize(self, connection):
        """disable all dialect initialization"""

    def do_execute(self, cursor, statement, parameters, context=None):
        try:
            stmt = context.compiled._panda_fn
        except AttributeError:
            raise exc.StatementError(
                "Only Pandas-compiled statements can be "
                "executed by this dialect", statement, parameters, None)
        cursor.execute(stmt, parameters)

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.namespace

dialect = PandasDialect
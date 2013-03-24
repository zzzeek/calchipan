from sqlalchemy.engine import default
from sqlalchemy import exc, __version__
from .compiler import PandasCompiler, PandasDDLCompiler
from . import dbapi
import warnings

class PandasExecutionContext(default.DefaultExecutionContext):
    def get_lastrowid(self):
        lrd = self.cursor.lastrowid
        if lrd == 0 and __version__ < "0.8.1":
            warnings.warn("lastrowid of zero won't function properly "
                    "with SQLAlchemy version < 0.8.1")
        return lrd


class PandasDialect(default.DefaultDialect):
    name = "pandas"
    driver = "calchipan"

    statement_compiler = PandasCompiler
    ddl_compiler = PandasDDLCompiler
    execution_ctx_cls = PandasExecutionContext

    # the first value we'd get for an autoincrement
    # column.
    default_sequence_base = 0

    def __init__(self, namespace=None, **kw):
        super(PandasDialect, self).__init__(**kw)
        self.namespace = namespace or {}

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

    def do_executemany(self, cursor, statement, parameters, context=None):
        try:
            stmt = context.compiled._panda_fn
        except AttributeError:
            raise exc.StatementError(
                "Only Pandas-compiled statements can be "
                "executed by this dialect", statement, parameters, None)
        cursor.executemany(stmt, parameters)

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.namespace

dialect = PandasDialect


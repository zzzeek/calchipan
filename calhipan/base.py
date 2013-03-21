from sqlalchemy.engine import default
from .compiler import PandasCompiler

class PandasDialect(default.DefaultDialect):
    statement_compiler = PandasCompiler

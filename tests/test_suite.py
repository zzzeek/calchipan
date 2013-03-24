from sqlalchemy.testing.suite.test_results import *
from sqlalchemy.testing.suite.test_insert import *
from sqlalchemy.testing.suite.test_reflection import *

from sqlalchemy import event, Column

@event.listens_for(Column, "after_parent_attach")
def _setup_autoincrement(column, table):
    if "test_needs_autoincrement" in column.info:
        table.kwargs['pandas_index_pk'] = True
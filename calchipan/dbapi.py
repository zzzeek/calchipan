"""A DBAPI (pep-249) emulation layer."""
import pandas as pd
from . import compat
import itertools


def connect(namespace=None):
    """Create a 'connection'.

    :param namespace: optional dictionary of names to pandas
     DataFrame objects.

    """
    return Connection(namespace)

paramstyle = 'named'

class Error(Exception):
    pass

class Connection(object):
    def __init__(self, namespace=None):
        self._namespace = namespace if namespace is not None else {}

    def add_namespace(self, name, dataframe):
        self._namespace[name] = dataframe

    def cursor(self):
        return Cursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass

class Cursor(object):
    _result = None
    lastrowid = None
    rowcount = None

    def __init__(self, connection):
        self.namespace = connection._namespace

    def execute(self, stmt, params=None):
        """Execute a 'statement'.

        The 'statement' here is a callable of the form::

                def execute(cursor, namespace, params):
                    ''

        Where ``cursor`` is the cursor,
        ``namespace`` is the namespace dictionary associated with the
        :class:`.Connection`, and ``params`` is the params dict passed
        here.  The callable should return a Pandas DataFrame object.

        """
        if isinstance(stmt, compat.basestring):
            raise Error("Only Pandas callable functions accepted for execute()")
        self._result = self.description = self.lastrowid = self.rowcount = None
        result = stmt(self, self.namespace, params)

        if isinstance(result, pd.DataFrame):
            self.dataframe = result
            self._rowset = result.itertuples(index=False)
            # type would be: result[k].dtype
            # but this isn't really compatible with DBAPI's
            # constant model; sqlite3 just returns None
            # so do that for now.
            self.description = [
                        (k, None, None, None, None, None, None)
                        for k in result.keys()]

    def executemany(self, stmt, multiparams):
        for param in multiparams:
            self.execute(stmt, param)

    def fetchone(self):
        try:
            return next(self._rowset)
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        size = size or 1
        return itertools.islice(self._rowset, 0, size)

    def fetchall(self):
        return list(self._rowset)

    def close(self):
        pass


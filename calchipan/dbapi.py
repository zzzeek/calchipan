"""A DBAPI (pep-249) emulation layer."""
import pandas as pd
import numpy as np
from . import compat
import itertools


def connect(namespace=None, trace=False):
    """Create a 'connection'.

    :param namespace: optional dictionary of names to pandas
     DataFrame objects.

    """
    return Connection(namespace, trace)

paramstyle = 'named'

class Error(Exception):
    pass

class Connection(object):
    def __init__(self, namespace=None, trace=False):
        self._namespace = {}
        self.api = PandasAPI(log=trace)
        if namespace:
            self._namespace.update(namespace)

    @property
    def trace(self):
        return self.api._buf

    def trace_as_string(self):
        return _print_trace(self.trace)

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
        self.api = connection.api

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

class PandasAPI(object):
    """Facade for Pandas methods; allows tracing of all function calls."""

    def __init__(self, log=False):
        self.log = log
        if log:
            self._buf = []

    def merge(self, df1, df2, **kw):
        df = pd.merge(df1, df2, **kw)
        if self.log:
            self._buf.append(("merge", df1, df2, df, kw))
        return df

    def concat(self, dfs, **kw):
        df = pd.concat(dfs, **kw)
        if self.log:
            self._buf.append(("concat", dfs, df))
        return df

    def rename(self, df, columns=None, inplace=False, copy=True):
        if self.log:
            self._buf.append(("rename", df, columns))
        return df.rename(columns=columns, inplace=inplace, copy=copy)

    def df_sort(self, df, **kw):
        if self.log:
            self._buf.append(("sort", df, kw))
        return df.sort(**kw)

    def dataframe(self, *arg, **kw):
        df = pd.DataFrame(*arg, **kw)
        if self.log:
            self._buf.append(("dataframe", df))
        return df

    def df_from_items(self, arg):
        if self.log:
            self._buf.append(("from_items", arg))
        return pd.DataFrame.from_items(arg)

    def to_string(self):
        return _print_trace(self)

def _print_trace(trace):
    def _df_str(df):
        return "%dx%d (%s)" % (len(df.keys()), len(df), ", ".join(df.keys()))

    def _merge_str(rec):
        return "Merge %s to %s to produce %s; %s" % (
                            _df_str(rec[1]),
                            _df_str(rec[2]),
                            _df_str(rec[3]),
                            rec[4],
                        )

    def _ones_str(rec):
        return "Array of %d ones" % rec[1]

    def _dataframe_str(rec):
        return "Create dataframe %s" % (_df_str(rec[1]))

    _trace_str = {
        "merge": _merge_str,
        "ones": _ones_str,
        "dataframe": _dataframe_str
    }

    lines = []
    for elem in trace:
        try:
            fn = _trace_str[elem[0]]
        except KeyError:
            pass
        else:
            lines.append(fn(elem))
    return "\n".join(lines)
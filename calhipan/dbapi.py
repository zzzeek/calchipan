"""A DBAPI (pep-249) emulation layer."""
import pandas as pd
import numpy as np

def connect(namespace=None, trace=None):
    """Create a 'connection'.

    :param namespace: optional dictionary of names to pandas
     DataFrame objects.

    """
    return Connection(namespace, trace)

class Connection(object):
    def __init__(self, namespace=None, trace=None):
        self._namespace = {}
        if trace is None:
            self.trace = Trace()
        else:
            self.trace = trace
        if namespace:
            self._namespace.update(namespace)

    def add_namespace(self, name, dataframe):
        self._namespace[name] = dataframe

    def cursor(self):
        return Cursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass

class Cursor(object):
    def __init__(self, connection):
        self.namespace = connection._namespace
        self.trace = connection.trace

    def execute(self, stmt, params=None, trace=None):
        if trace is None:
            trace = self.trace
        result = stmt(trace, self.namespace, params)
        self._result = [tuple(rec) for rec in result.to_records(index=False)]
        # type would be: result[k].dtype
        # but this isn't really compatible with DBAPI's
        # constant model; sqlite3 just returns None
        # so do that for now.
        self.description = [
                    (k, None, None, None, None, None, None)
                    for k in result.keys()]

    def executemany(self, stmt, multiparams):
        raise NotImplementedError()

    def fetchone(self):
        return self._result.pop(0)

    def fetchmany(self, size=None):
        size = size or 1
        ret = self._result[0:size]
        self._result[:size] = []
        return ret

    def fetchall(self):
        ret = self._result[:]
        self._result[:] = []
        return ret

    def close(self):
        pass

class Trace(object):
    def __init__(self, log=False):
        self.log = log
        if log:
            self._buf = []

    def merge(self, df1, df2, **kw):
        df = pd.merge(df1, df2, **kw)
        if self.log:
            self._buf.append(("merge", df1, df2, df, kw))
        return df

    def concat(self, dfs):
        df = pd.concat(dfs)
        if self.log:
            self._buf.append(("concat", dfs, df))
        return df

    def rename(self, df, columns=None, inplace=False):
        return df.rename(columns=columns, inplace=inplace)

    def np_ones(self, length):
        if self.log:
            self._buf.append(("ones", length))
        return np.ones(length)

    def reset_index(self, col, **kw):
        if self.log:
            self._buf.append(("reset_index", col, kw))
        return col.reset_index(**kw)

    def df_getitem(self, df, expr):
        if self.log:
            self._buf.append(("getitem", df, expr))
        return df[expr]

    def df_index(self, df):
        if self.log:
            self._buf.append(("index", df))
        return df.index

    def df_ix_getitem(self, df, expr):
        if self.log:
            self._buf.append(("ix_getitem", df, expr))
        return df.ix[expr]

    def dataframe(self, *arg, **kw):
        df = pd.DataFrame(*arg, **kw)
        if self.log:
            self._buf.append(("dataframe", df))
        return df

    def df_from_items(self, arg):
        if self.log:
            self._buf.append(("from_items", arg))
        return pd.DataFrame.from_items(arg)

    def _df_str(self, df):
        return "%dx%d (%s)" % (len(df.keys()), len(df), ", ".join(df.keys()))

    def _merge_str(self, rec):
        return "Merge %s to %s to produce %s; %s" % (
                            self._df_str(rec[1]),
                            self._df_str(rec[2]),
                            self._df_str(rec[3]),
                            rec[4],
                        )

    def _ones_str(self, rec):
        return "Array of %d ones" % rec[1]

    def _dataframe_str(self, rec):
        return "Create dataframe %s" % (self._df_str(rec[1]))

    _trace_str = {
        "merge": _merge_str,
        "ones": _ones_str,
        "dataframe": _dataframe_str
    }
    def to_string(self):
        lines = []
        for elem in self._buf:
            try:
                fn = self._trace_str[elem[0]]
            except KeyError:
                pass
            else:
                lines.append(fn(self, elem))
        return "\n".join(lines)
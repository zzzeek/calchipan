"""A DBAPI (pep-249) emulation layer."""
import numpy as np


def connect(namespace=None):
    """Create a 'connection'.

    :param namespace: optional dictionary of names to pandas
     DataFrame objects.

    """
    return Connection(namespace)

class Connection(object):
    def __init__(self, namespace=None):
        self._namespace = {}
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

    def execute(self, stmt, params=None):
        result = stmt(self.namespace, params)
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
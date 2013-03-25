def in_op(a, b):
    # assume a is a series for the moment,
    # b is a straight list
    return a.map(lambda x: x in b)

def is_op(a, b):
    if b is None:
        return a.isnull()
    else:

        raise NotImplementedError()

def isnot_op(a, b):
    if b is None:
        return a.notnull()
    else:

        raise NotImplementedError()
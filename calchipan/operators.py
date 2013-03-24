def in_op(a, b):
    # assume a is a series for the moment,
    # b is a straight list
    return a.map(lambda x: x in b)
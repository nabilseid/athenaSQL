from athenaSQL.column import WindowColumn

from athenaSQL.functions.functions import _create_window_function

_window_functions = {
    'cume_dist': 'Returns the cumulative distribution of a value in a group of values',
    'dense_rank': 'Returns the rank of a value in a group of values',
    'percent_rank': 'Returns the percentage ranking of a value in group of values',
    'rank': 'Returns the rank of a value in a group of values',
    'row_number': 'Returns a unique sequential number for each row'
}

for _name, _doc in _window_functions.items():
    globals()[_name] = _create_window_function(_name, _doc)

# ntile(n)


def ntile(n):
    """
    Divides the rows for each window partition into n buckets ranging 
    from 1 to at most n.
    """

    if not isinstance(n, int):
        raise TypeError('`{type(n).__name__}` is not of a type `int`')

    wCol = WindowColumn('ntile', n)
    return wCol

# first_value(x)


def first_value(x):
    """
    """
    wCol = WindowColumn('first_value', x)
    return wCol

# last_value(x)


def last_value(x):
    """
    """
    wCol = WindowColumn('last_value', x)
    return x

#nth_value(x, offset)


def nth_value(x, offset):
    """
    """
    wCol = WindowColumn('nth_value', x, offset)
    return wCol

# lead(x[, offset[, default_value]])


def lead(x, offset=None, default_value=None):
    """
    """
    if default_value and not offset:
        raise ValueError('`default_value` is provided without `offset`')

    optional_args = [arg for arg in [offset, default_value] if arg]

    wCol = WindowColumn('lead', x, *optional_args)

    return wCol

# lag(x[, offset[, default_value]])


def lag(x, offset, default_value):
    """
    """
    if default_value and not offset:
        raise ValueError('`default_value` is provided without `offset`')

    optional_args = [arg for arg in [offset, default_value] if arg]

    wCol = WindowColumn('lag', x, *optional_args)

    return wCol


# Ensure that Sphinx finds the dynamically created functions
__all__ = [
    *_window_functions.keys(),
    'ntile',
    'first_value',
    'last_value',
    'nth_value',
    'lead',
    'lag'
]

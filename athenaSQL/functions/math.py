from athenaSQL.column import Column, AggregateColumn

from athenaSQL.functions.functions import _create_unary_function, \
    _create_binary_function

_unary_functions = {}

_binary_functions = {}

for _name, _doc in _unary_functions.items():
    globals()[_name] = _create_unary_function(_name, _doc)

for _name, _doc in _binary_functions.items():
    globals()[_name] = _create_binary_function(_name, _doc)


def round(col, d=None):
    """
    Returns col rounded to d decimal places.
    Round col to the nearest integer if d is not specified.
    """

    if not isinstance(col, (Column, str)):
        raise TypeError('col argument in replace should be a type of '
                        '[Column|str]')

    if isinstance(col, str):
        col = Column(col)

    col._sql_clause = f"ROUND({col}"
    col._sql_clause += f", {d})" if d else ")"

    return col


# Ensure that Sphinx finds the dynamically created functions
__all__ = [
    *_unary_functions.keys(),
    *_binary_functions.keys(),
    'round'
]

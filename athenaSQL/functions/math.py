from adflow.sql.column import Column, AggregateColumn

from adflow.sql.functions.functions import _create_unary_function, \
        _create_binary_function, \
        _create_nullnary_function

from adflow.sql.queries.query_abc import _check_and_extract_list_or_valid_typed_arguments

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

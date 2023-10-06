from adflow.sql.column import Column, AggregateColumn

from adflow.sql.functions.functions import _create_unary_function, \
        _create_binary_function, \
        _create_nullnary_function

_unary_functions = {
    'any_value': 'Returns an arbitrary non-null value x, if one exists',
    'arbitrary': 'Returns an arbitrary non-null value of x, if one exists. Identical to any_value().',
    'array_agg': 'Returns an array created from the input x elements.',
    'avg': 'Returns the average (arithmetic mean) of all input values.',
    'bool_and': 'Returns TRUE if every input value is TRUE, otherwise FALSE.',
    'bool_or': 'Returns TRUE if any input value is TRUE, otherwise FALSE.',
    'checksum': 'Returns an order-insensitive checksum of the given values.',
    'count': 'Returns the number of non-null input values.',
    'count_if': 'Returns the number of TRUE input values. This function is equivalent to count(CASE WHEN x THEN 1 END).',
    'every': 'This is an alias for bool_and().',
    'geometric_mean': 'Returns the geometric mean of all input values.',
    'max': 'Returns the maximum value of all input values.',
    'min': 'Returns the minimum value of all input values.',
    'sum': 'Returns the sum of all input values.'
}

_binary_functions = {}

for _name, _doc in _unary_functions.items():
    globals()[_name] = _create_unary_function(_name, _doc, AggregateColumn)

for _name, _doc in _binary_functions.items():
    globals()[_name] = _create_binary_function(_name, _doc, AggregateColumn)



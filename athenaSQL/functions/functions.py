from athenaSQL.column import Column, NewColumn, \
    ConditionalColumn, CaseColumn, WindowColumn

from athenaSQL.queries.query_abc import (
    _check_and_extract_list_or_valid_typed_arguments)

from athenaSQL.data_type import DataType

def _wrap_col(col, wrapper, parent_wrapper):
    """
    wrap `col` with a `wrapper` class. `wrapper` should be of a type `wrapper_type`.
    """
    if not issubclass(wrapper, parent_wrapper):
        raise TypeError(f'{wrapper} is not a type of {parent_wrapper}')

    return wrapper(col)


def _create_unary_function(name, doc="", func_type=None):
    """ Create a function for unary function by name"""
    def _(col):

        # if col is string constract column before operation
        if isinstance(col, str):
            col = Column(col)

        if not isinstance(col, Column):
            raise TypeError(f'{col} argument is not type of a `Column`')

        col._sql_clause = (f'{name.upper()}({col})')

        # wrap col with custom functional column type
        if func_type:
            col = func_type(col)
            #col = _wrap_col(col, func_type, FunctionalColumn)

        return col

    _.__name__ = name
    _.__doc__ = doc
    return _


def _create_binary_function(name, doc="", func_type=None):
    """Create a function for binary function by name"""
    def _(col, operand):

        # if col is string constract column before operation
        if isinstance(col, str):
            col = Column(col)

        if not isinstance(col, Column):
            raise TypeError(f'{col} argument is not type of a `Column`')

        # enclose string arguments with single quotes
        # FUNC(col, operand), FUNC('col', 'operand')
        col._sql_clause = (f'{name.upper()}({col}, '
                           + (f'\'{operand}\'' if isinstance(operand,
                              str) else str(operand))
                           + ')')

        # wrap col with custom functional column type
        if func_type:
            col = func_type(col)
            #col = _wrap_col(col, func_type, FunctionalColumn)

        return col

    _.__name__ = name
    _.__doc__ = doc
    return _


def _create_nullnary_function(name, doc=""):
    """ """
    # TODO nullary functions like pi(), e()
    pass


def _create_window_function(name, doc=""):
    """Create  """
    def _():
        wCol = WindowColumn(name)
        return wCol

    _.__name__ = name
    _.__doc__ = doc
    return _


_unary_functions = {
    'sqrt': 'Computes the square root of the specified float value.',
    'abs': 'Computes the absolute value.',
    'mean': 'Aggregate function: returns the average of the values in a group.',
    'geometric_mean': 'Returns the geometric mean of all input values.',
    'stddev': 'Returns the sample standard deviation of all input values.',
    'variance': 'Returns the sample variance of all input values.',
    # Unicode Functions
    # opt argument
    'normalize': 'Transforms string with NFC normalization form.',
    'to_utf8': 'Encodes string into a UTF-8 varbinary representation.',
    # opt argument
    'from_utf8': 'Decodes a UTF-8 encoded string from binary. Invalid UTF-8 '
                 'sequences are replaced with the Unicode replacement character U+FFFD.',
    # URL Functions
    # [protocol:][//host[:port]][path][?query][#fragment]
    'url_extract_fragment': 'Returns the fragment identifier from url.',
    'url_extract_host': 'Returns the host from url.',
    'url_extract_path': 'Returns the path from url.',
    'url_extract_port': 'Returns the port number from url.',
    'url_extract_protocol': 'Returns the protocol from url.',
    'url_extract_query': 'Returns the query string from url.',
    'url_encode': '',
    'url_decode': 'Unescapes the URL encoded value. This function is the '
                  'inverse of url_encode().',
    # UUID Functions
    'uuid': 'Returns a pseudo randomly generated UUID (type 4).',

}

_binary_functions = {
    'nullif': 'Returns null if value1 equals value2, otherwise returns value1.',
    'mod': 'Returns the modulus (remainder) of n divided by m.',
    'power': 'Returns x raised to the power of p.',
    'pow': 'This is an alias for power().'
}

for _name, _doc in _unary_functions.items():
    globals()[_name] = _create_unary_function(_name, _doc)

for _name, _doc in _binary_functions.items():
    globals()[_name] = _create_binary_function(_name, _doc)

# Aliases for creating a column class by name
col = column = lambda col_name: Column(col_name)
col.__doc__ = column.__doc__ = "Create a column by name."

# Aliase for creating a new column with type


def nCol(col_name, data_type): return NewColumn(col_name, data_type)


def when(condition, value):
    """
    Start a case condition. Returns a CaseColumn 
    """
    if not isinstance(condition, ConditionalColumn):
        raise TypeError('condition argument in when() should be a '
                        'ConditionalColumn')

    return CaseColumn((condition, value))


def cast(col, _type):
    """Alias for Column().cast()."""

    # if col is string constract column before operation
    if isinstance(col, str):
        col = Column(col)

    if not isinstance(col, Column):
        raise TypeError('col argument in cast() should be a Column')

    return col.cast(_type)


def try_cast(col: Column, _type: DataType):
    """Like cast(), but returns null if the cast fails.

    Args:
        col (Column): A column to be casted.
        _type (DataType): An enum of supported data type to cast to.

    Raises:
        TypeError: raised if col is not a type of Column.

    Returns:
        Column: return a casted col.
    """

    # if col is string constract column before operation
    if isinstance(col, str):
        col = Column(col)

    if not isinstance(col, Column):
        raise TypeError('col argument in try_cast() should be a Column')

    return col.cast(_type, _try=True)


# Ensure that Sphinx finds the dynamically created functions
__all__ = [
    *_unary_functions.keys(),
    *_binary_functions.keys(),
    'col',
    'column',
    'nCol',
    'when',
    'cast',
    'try_cast']
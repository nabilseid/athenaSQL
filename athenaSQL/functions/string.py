from athenaSQL.column import Column

from athenaSQL.functions.functions import _create_unary_function, \
    _create_binary_function

from athenaSQL.queries.query_abc import _check_and_extract_list_or_valid_typed_arguments

_unary_functions = {
    'chr': 'Returns the Unicode code point n as a single character string.',
    'codepoint': 'Returns the Unicode code point of the only character of string.',
    'length': 'Returns the length of string in characters.',
    'upper': 'Converts a string expression to upper case.',
    'lower': 'Converts a string expression to upper case.',
    'ltrim': 'Removes leading whitespace from string.',
    'reverse': 'Returns string with the characters in reverse order.',
    'rtrim': 'Removes trailing whitespace from string.',
    'trim': 'Removes leading and trailing whitespace from string.',
    'word_stem': 'Returns the stem of word in the English language.'
}

_binary_functions = {
    'hamming_distance': 'Returns the Hamming distance of string1 and string2, '
                        'i.e. the number of positions at which the corresponding '
                        'characters are different. Note that the two strings '
                        'must have the same length.',
    'levenshtein_distance': 'Returns the Levenshtein edit distance of string1 '
                            'and string2, i.e. the minimum number of single-character '
                            'edits (insertions, deletions or substitutions) needed '
                            'to change string1 into string2.'
}


for _name, _doc in _unary_functions.items():
    globals()[_name] = _create_unary_function(_name, _doc)

for _name, _doc in _binary_functions.items():
    globals()[_name] = _create_binary_function(_name, _doc)


def coalesce(*cols):
    """
    """

    arguments = _check_and_extract_list_or_valid_typed_arguments(cols,
                                                                 'coalesce', valid_types=(str, Column))
    
    column = Column('_')
    column._sql_clause = f'COALESCE(' 
    for index, arg in enumerate(arguments, start=1):
        column._sql_clause += (f'\'{arg}\''
                               if isinstance(arg, str)
                               else f'{arg}')

        if index != len(arguments):
            column._sql_clause += ', '

    column._sql_clause += ')'

    return column
    
def concat(*cols):
    """
    """

    arguments = _check_and_extract_list_or_valid_typed_arguments(cols,
                                                                 'concat', valid_types=(str, Column))

    column = Column('_')
    column._sql_clause = f'CONCAT('
    for index, arg in enumerate(arguments, start=1):
        column._sql_clause += (f'\'{arg}\''
                               if isinstance(arg, str)
                               else f'{arg}')

        if index != len(arguments):
            column._sql_clause += ', '

    column._sql_clause += ')'

    return column


def replace(col, search, replace=None):
    """Replaces all instances of search with replace in col.
    """

    if not isinstance(col, (Column, str)):
        raise TypeError('col argument in replace should be a type of '
                        '[Column|str]')

    if isinstance(col, str):
        col = Column(col)

    # TODO type check search and replace

    col._sql_clause = f"REPLACE({col}, '{search}'"
    col._sql_clause += f", '{replace}')" if replace!=None else ")"

    return col


def substring(col, start, length=None):
    """
    Returns a substring from string of length length from the starting 
    position start.Positions start with 1. A negative starting position 
    is interpreted as being relative to the end of the string.
    """

    if not isinstance(col, (Column, str)):
        raise TypeError('col argument in replace should be a type of '
                        '[Column|str]')

    if isinstance(col, str):
        col = Column(col)

    # TODO type check 'start' and 'length'

    col._sql_clause = f"SUBSTRING({col}, {start}"
    col._sql_clause += f", {length})" if length else ")"

    return col


# Ensure that Sphinx finds the dynamically created functions
__all__ = [
    *_unary_functions.keys(),
    *_binary_functions.keys(),
    'coalesce',
    'concat',
    'replace',
    'substring'
]

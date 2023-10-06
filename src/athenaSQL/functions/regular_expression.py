from adflow.sql.column import ConditionalColumn

from adflow.sql.functions.functions import _create_unary_function, \
        _create_binary_function, \
        _create_nullnary_function

_binary_functions = {
    'regexp_count': 'Returns the number of occurrence of pattern in string.',
    'regexp_extract_all': 'Returns the substring(s) matched by the regular expression pattern in string.',
    'regexp_extract': 'Returns the first substring matched by the regular expression pattern in string.',
    'regexp_position': 'Returns the index of the first occurrence (counting from 1) of pattern in string. Returns -1 if not found.',
    'regexp_replace': 'Removes every instance of the substring matched by the regular expression pattern from string.',
    'regexp_split': 'Splits string using the regular expression pattern and returns an array. Trailing empty strings are preserved.'
}

for _name, _doc in _binary_functions.items():
    globals()[_name] = _create_binary_function(_name, _doc)


_conditioal_binary_functions = {
    'regexp_like': 'Evaluates the regular expression pattern and determines if it is contained within string.'
}


for _name, _doc in _conditioal_binary_functions.items():
    globals()[_name] = _create_binary_function(_name,
                                               _doc,
                                               func_type=ConditionalColumn)


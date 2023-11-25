from athenaSQL.column import Column

from athenaSQL.functions.functions import _create_unary_function, \
    _create_binary_function

_unary_functions = {
    # Date and Time Functions
    'date': 'This is an alias for CAST(x AS date).',
    'last_day_of_month': 'Returns the last day of the month.',
    'from_iso8601_timestamp': 'Parses the ISO 8601 formatted string into a '
                              'timestamp with time zone.',
    # Date and Time Convenience Extraction Functions
    'day': 'Returns the day of the month from x.',
    'day_of_month': 'This is an alias for day().',
    'day_of_week': 'Returns the ISO day of the week from x. The value ranges '
                   'from 1 (Monday) to 7 (Sunday).',
    'day_of_year': 'Returns the day of the year from x. The value ranges from 1 to 366.',
    'dow': 'This is an alias for day_of_week().',
    'doy': 'This is an alias for day_of_year().',
    'hour': 'Returns the hour of the day from x. The value ranges from 0 to 23.',
    'millisecond': 'Returns the millisecond of the second from x.',
    'minute': 'Returns the minute of the hour from x.',
    'month': 'Returns the month of the year from x.',
    'quarter': 'Returns the quarter of the year from x. The value ranges from '
               '1 to 4.',
    'second': 'Returns the second of the minute from x.',
    'timezone_hour': 'Returns the hour of the time zone offset from timestamp.',
    'timezone_minute': 'Returns the minute of the time zone offset from timestamp.',
    'week': 'Returns the ISO week of the year from x. The value ranges from 1 to 53.',
    'week_of_year': 'This is an alias for week().',
    'year': 'Returns the year from x.',
    'year_of_week': 'Returns the year of the ISO week from x.',
    'yow': 'This is an alias for year_of_week().',
}

_binary_functions = {}

for _name, _doc in _unary_functions.items():
    globals()[_name] = _create_unary_function(_name, _doc)

for _name, _doc in _binary_functions.items():
    globals()[_name] = _create_binary_function(_name, _doc)
    
# supported athena data types
# https://trino.io/docs/current/functions/datetime.html#interval-functions
supported_units = ['millisecond', 'second', 'minute', 'hour', 'day', 
                    'week', 'month', 'quarter', 'year']
    
def date_diff(unit, timestamp1, timestamp2):
    
    # lowercase dataType to check if it is supported
    unit = unit.lower()
    
    if unit not in supported_units:
        raise TypeError(f'unsupported unit {dataType}')
    
    if not isinstance(timestamp1, (Column, str)):
        raise TypeError('timestamp1 argument in date_diff() should be a Column or Str')
    
    if not isinstance(timestamp2, (Column, str)):
        raise TypeError('timestamp2 argument in date_diff() should be a Column or Str')
    
    _col = Column('_')
    _col._sql_clause = f'date_diff(\'{unit}\', {timestamp1}, {timestamp2})'
    
    return _col

def date_trunc(unit, col):
    
    # lowercase dataType to check if it is supported
    unit = unit.lower()
    
    if unit not in supported_units:
        raise TypeError(f'unsupported unit {dataType}')
    
    if not isinstance(col, (Column, str)):
        raise TypeError('col argument in date_trunc() should be a Column or Str')
    
    col._sql_clause = f'date_trunc(\'{unit}\', {col})'
    
    return col

# Ensure that Sphinx finds the dynamically created functions
__all__ = [
    *_unary_functions.keys(),
    *_binary_functions.keys(),
    'date_diff',
    'date_trunc'
]
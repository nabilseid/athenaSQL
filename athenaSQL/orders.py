from adflow.sql.column import validate_column_name

class ASC:
    def __init__(self, column):
        self._column = validate_column_name(column)

    def __str__(self):
        return f'{self._column} DESC'

class DESC:
    def __init__(self, column):
        self._column = validate_column_name(column)

    def __str__(self):
        return f'{self._column} ASC'


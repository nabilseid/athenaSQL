import re
import copy
from athenaSQL.column_type import ColumnType
from athenaSQL.operator_mixin import ComparisonMixin, \
    ArithmeticMixin, \
    LogicalMixin


def validate_column_name(column):
    """
    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html
    """

    # column contains special characters other than underscore
    if not re.match(r'^[A-Za-z0-9_*]+$', column):
        raise ValueError(
            f'{column} is not a valid column name. '
            'Only alphanumeric & underscore (_) are supported.')

    # convert column name to lower case. All column in Athena are lowercase
    column = column.lower()

    return column


def areinstances(values, _type):
    if not isinstance(values, (list, tuple)):
        values = [values]
    return all(map(lambda v: isinstance(v, _type), values))


class Column(ComparisonMixin, ArithmeticMixin, LogicalMixin):
    """
    Athena column abstraction class.

    Parameters
    ----------
    col: str
        desired column name
    """

    def __init__(self, col):
        self.column = validate_column_name(col)

        self._sql_clause = col

    def __str__(self):
        return f'{self._sql_clause}'

    def isin(self, *cols):
        """
        """
        if len(cols) == 1 and isinstance(cols[0], (list, set)):
            cols = cols[0]

        self._sql_clause = f'{self._sql_clause} IN ('
        for col in cols:
            if isinstance(col, str):
                self._sql_clause = f"{self._sql_clause}'{col}', "
                continue

            self._sql_clause = f'{self._sql_clause}{col}, '

        # [:-2] remove the last ,
        self._sql_clause = f'{self._sql_clause[:-2]})'

        return ConditionalColumn(self)

    def isNull(self):
        """
        """
        self._sql_clause = f'{self._sql_clause} IS NULL'
        return ConditionalColumn(self)

    def isNotNull(self):
        """
        """
        self._sql_clause = f'{self._sql_clause} IS NOT NULL'
        return ConditionalColumn(self)

    def alias(self, alias):
        """
        Add alias for a column.

        Example
        -------
        >>> Column('age') + 10 
        age + 10
        >>> (Column('age') + 10).alias('age10')
        age + 10 AS age10
        """
        # cast alias to string
        alias = str(alias)

        self._sql_clause += f' AS {alias}'

        return AliasColumn(alias, host=self)

    def cast(self, dataType, _try=False):
        """
        """

        # lowercase dataType to check if it is supported
        dataType = dataType.lower()

        # supported athena data types
        # https://docs.aws.amazon.com/athena/latest/ug/data-types.html
        supported_types = ['boolean', 'tinyint', 'smallint', 'int', 'integer',
                           'bigint', 'double', 'float', 'char', 'varchar',
                           'string', 'binary', 'date', 'timestamp', 'decimal']

        if dataType not in supported_types:
            raise TypeError(f'unsupported data type {dataType}')

        if _try:
            self._sql_clause = f'TRY_CAST({self._sql_clause} AS {dataType.upper()})'
        else:
            self._sql_clause = f'CAST({self._sql_clause} AS {dataType.upper()})'

        clone_obj = copy.deepcopy(self)
        return clone_obj

    def between(self, lowerBound, upperBound):
        """
        """
        # TODO check if params are int or str, import from other modules
        if not (areinstances([lowerBound, upperBound], str) or
                areinstances([lowerBound, upperBound], int)):
            raise TypeError('unsupported mixed types, '
                            'supported types are [str] or [int]')

        range_clause = f'{lowerBound} AND {upperBound}'
        if areinstances([lowerBound, upperBound], str):
            range_clause = f"'{lowerBound}' AND '{upperBound}'"

        self._sql_clause = f'{self._sql_clause} BETWEEN {range_clause}'

        return ConditionalColumn(self)

    def when(self, condition, value):
        """
        A method to give an exception instraction how to use when condition.
        """
        raise ValueError('when() can only be applied on a Column previously '
                         'generated by when() function')

    def otherwise(self, value):
        """
        A method to give an exception instraction how to use otherwise()
        condition.
        """
        raise ValueError('otherwise() can only be applied on a Column '
                         'previously generated by when() function')

    def over(self, window):
        """
        """

        if type(self) not in [AggregateColumn, WindowColumn]:
            raise TypeError('`over()` can only be applied to `aggregated` or '
                            '`window` columns.')

        # scope error
        from athenaSQL.queries.window import WindowQuery
        if not isinstance(window, WindowQuery):
            raise ValueError(f'`{type(window).__name__}` is not a type '
                             f'of `WindowColumn`')

        clone_obj = copy.deepcopy(self)
        clone_obj._sql_clause = f'{self._sql_clause} {window._to_sql()}'

        return clone_obj


class NewColumn(Column):
    """
    A column with column name and it's data type.
    used when creating a new table.
    """

    def __init__(self, col_name, data_type):
        super().__init__(col_name)

        if not isinstance(data_type, ColumnType):
            raise TypeError(f'{data_type} is not a type of ColumnType.')

        self.column_type = data_type

        self._sql_clause = f'`{col_name}` {data_type.__str__()}'


class ConditionalColumn(Column):
    """
    """

    def __init__(self, col):
        super().__init__(col.column)

        self._sql_clause = col.__str__()


class FunctionalColumn(Column):
    """
    A function wrapping a column.
    https://trino.io/docs/current/functions.html
    """

    def __init__(self, col):
        super().__init__(col.column)

        self._sql_clause = col.__str__()


class AggregateColumn(FunctionalColumn):
    """https://trino.io/docs/current/functions/aggregate.html
    """

    def __init__(self, col):
        super().__init__(col)


class WindowColumn(Column):
    """https://trino.io/docs/current/functions/window.html
    """

    def __init__(self, func_name, *args):
        super().__init__('_')
        # take str representation of args and
        # remove leading and trailing parentheses
        self._sql_clause = f'{func_name}({str(list(args))[1:-1]})'


class AliasColumn:
    """
    A column class with alias. It is a return type for Column.alias() function.
    It doesn't support any further chaining of column functionalities.
    """

    def __init__(self, alias, *, host):
        # column type of the alias attached to
        # used to identify column type of the host
        self.host = host
        self.column = alias
        self._sql_clause = host.__str__()

    def __str__(self):
        return f'{self._sql_clause}'


class CaseColumn(Column):
    """
    """

    def __init__(self, first_case):
        self.when_cases = [first_case]
        self.else_case = None

        self._sql_clause = self._build_case_sql()

    def _build_case_sql(self):
        """
        """
        case_clause = 'CASE'

        for (condition, value) in self.when_cases:
            case_clause += f'\n\tWHEN {condition} THEN '
            case_clause += (f'\n\t\t\'{value}\'' if isinstance(value, str)
                            else f'\n\t\t{value}')

        if not isinstance(self.else_case, type(None)):
            case_clause += f'\n\tELSE '
            case_clause += (f'\n\t\t\'{self.else_case}\''
                            if isinstance(self.else_case, str)
                            else f'\n\t\t{self.else_case}')

        case_clause += '\nEND'

        return case_clause

    def when(self, condition, value):
        """
        """
        if not isinstance(condition, ConditionalColumn):
            raise TypeError('condition argument in when() should be a '
                            'ConditionalColumn')

        # TODO type checker for value, but value can be wide range of things
        self.when_cases.append((condition, value))
        self._sql_clause = self._build_case_sql()

        clone_obj = copy.deepcopy(self)
        return clone_obj

    def otherwise(self, value):
        """
        """

        # TODO type checker for value
        self.else_case = value

        _column = Column('_')
        _column._sql_clause = self._build_case_sql()

        return _column

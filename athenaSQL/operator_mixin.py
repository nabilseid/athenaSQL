def _exit_on_invalid_operand(other):
    """Exit if operand is not of a type int, str or Column
    """
    from athenaSQL.column import Column

    if not isinstance(other, (int, str, Column)):
        raise TypeError(
            f"Column can't be compared with {type(other).__name__}")


def col_boolOps(boolOps, compound=False):
    """
    A decorator to check unsupported operands for [&,|] ops and 
    change return type to ConditionalColumn if ops is bool 
    """
    def decorator(func):
        def wrapper(self, other):
            obj = func(self, other)

            from athenaSQL.column import ConditionalColumn
            # if operand is not ConditionalColumn and operation is boolOps
            if boolOps and compound and type(self) != ConditionalColumn:
                raise TypeError(f"unsupported operand type(s): "
                                f"'{type(self).__name__}' and "
                                f"'{type(other).__name__}'")

            if boolOps:
                return ConditionalColumn(self)

            return obj

        return wrapper

    return decorator


def _bin_op(operator, r=False, compound=False, boolOps=False):
    """Create a method for given binary operation
    """

    @col_boolOps(boolOps, compound=compound)
    def _(self, other):
        _exit_on_invalid_operand(other)

        # if operand is string enclose it in single quotes
        if isinstance(other, str):
            other = f"'{other}'"

        # self is on the left, operand is on the right
        # sum('col') + 6
        clause = f'{self._sql_clause} {operator} {other}'

        # self is on the right, operand is on the left
        # 6 + sum('col')
        if r:
            clause = f'{other} {operator} {self._sql_clause}'

        # operation is compound, it needs to be enclosed with parenthesis
        # (col > 5 AND col2 = 10)
        if compound:
            clause = f'({clause})'

        self._sql_clause = clause

        return self
    return _


def _func_op(operator, boolOps=False):
    """
    Create a method for given function operations

    Example
    -------
    >>> from athenaSQL.function import avg
    >>> ~(avg('col') > 5)
    NOT(AVG(col) > 5)
    """

    def _(self):
        from athenaSQL.column import ConditionalColumn
        # if operand is not ConditionalColumn and operation is boolOps
        if boolOps and type(self) != ConditionalColumn:
            raise TypeError(f'unsupported operand type: {type(self).__name__}')

        self._sql_clause = f'{operator}({self._sql_clause})'

        # if operation is conditional or logical return ConditionalColumn type
        if boolOps:
            return ConditionalColumn(self)

        return self
    return _


def _bin_func_op(operator, r=False, boolOps=False):
    """
    """

    @col_boolOps(boolOps)
    def _(self, other):
        _exit_on_invalid_operand(other)

        if isinstance(other, str):
            other = f"'{other}'"

        # r is False, self is the left operand of the binary funciton operator
        # POWER(SUM(col), 2)
        clause = f'{operator}({self._sql_clause}, {other})'

        # self is the right operand of the binary function operation
        # POWER(2, SUM(col))
        if r:
            clause = f'{operator}({other}, {self._sql_clause})'

        self._sql_clause = clause

        return self
    return _


class ComparisonMixin(object):
    """
    A mixin class to enable comparison operators on SQLFunctions

    Example
    -------
    >>> From athenaSQL.interface.function import sum
    >>> str(sum('col'))
    SUM('col')
    >>> str(sum('col') >= 10)
    SUM('col') >= 10
    """

    # comparison operators
    __lt__ = _bin_op('<', boolOps=True)
    __le__ = _bin_op('<=', boolOps=True)
    __eq__ = _bin_op('=', boolOps=True)
    __gt__ = _bin_op('>', boolOps=True)
    __ge__ = _bin_op('>=', boolOps=True)
    __ne__ = _bin_op('<>', boolOps=True)


class ArithmeticMixin(object):
    """
    A mixin class to enable arithmetic operations on SQLFunctions

    Example
    -------
    >>> From athenaSQL.interface.function import sum
    >>> str(sum('col'))
    SUM('col')
    >>> str(sum('col') + 10)
    SUM('col') + 10
    """

    # arithmetic operations
    __add__ = _bin_op("+")
    __sub__ = _bin_op("-")
    __mul__ = _bin_op("*")
    __mod__ = _bin_op("%")
    __truediv__ = _bin_op("/")
    __radd__ = _bin_op("+", r=True)
    __rsub__ = _bin_op("-", r=True)
    __rmul__ = _bin_op("*", r=True)
    __rmod__ = _bin_op("%", r=True)
    __rtruediv__ = _bin_op("/", r=True)

    # functional arithmetic operations
    __pow__ = _bin_func_op("POWER")
    __rpow__ = _bin_func_op("POWER", r=True)
    __abs__ = _func_op("ABS")

    # TODO __round__
    # TODO __trunc__
    # TODO __floor__
    # TODO __ceil__


class LogicalMixin(object):
    """
    A mixin class to enable logical arithmetic operators on SQLFunctions

    Example
    -------
    >>> FROM athenaSQL.function import sum, avg
    >>> str(sum('col'))
    SUM('col')
    >>> str(sum('col') == 10 AND avg('col2') == 5)
    (SUM('col') = 10 AND AVG('col2') = 5)
    """

    # logical operators
    __and__ = _bin_op("AND", compound=True, boolOps=True)
    __or__ = _bin_op("OR", compound=True, boolOps=True)
    __invert__ = _func_op("NOT", boolOps=True)
    __rand__ = _bin_op("AND", r=True, compound=True, boolOps=True)
    __ror__ = _bin_op("OR", r=True, compound=True, boolOps=True)

from athenaSQL.column import Column, NewColumn, \
    ConditionalColumn, CaseColumn, WindowColumn

from athenaSQL.utils import stringify

def ifTrue(condition, true_value, false_value=None):

    if not isinstance(condition, ConditionalColumn):
        raise TypeError(f'{condition} argument is not type of a `Column`')
    
    _false_value = isinstance(false_value, Column) or false_value != None

    condition._sql_clause = (f'IF({condition}, {true_value}'
                             f', {stringify(false_value)})' if _false_value != None else ')')

    return condition

def tryOrNull(expression):
    """Evaluate an expression and handle certain types of errors by returning NULL.
    The sql function identifier is `TRY(expression)`.

    Args:
        expression Column: a sql expression

    Raises:
        TypeError: If expression is not type of Column will raise TypeError

    Returns:
        Column: expression wrapped with `TRY` function
    """
    if not isinstance(expression, Column):
        raise TypeError(f'{expression} argument is not type of a `Column`')
    
    expression._sql_clause = (f'TRY({expression})')
    
    return expression


# Ensure that Sphinx finds the dynamically created functions
__all__ = [
    'ifTrue',
    'tryOrNull'
]
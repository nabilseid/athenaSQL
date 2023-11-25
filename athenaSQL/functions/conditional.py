from athenaSQL.column import Column, NewColumn, \
    ConditionalColumn, CaseColumn, WindowColumn


def ifTrue(condition, true_value, false_value=None):

    if not isinstance(condition, Column):
        raise TypeError(f'{condition} argument is not type of a `Column`')
    
    condition._sql_clause = (f'IF({condition}, {true_value}'
                             f', {false_value})' if false_value != None else ')')

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
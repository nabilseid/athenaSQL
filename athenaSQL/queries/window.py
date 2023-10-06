import copy
from jinja2 import Template

from athenaSQL.queries.query_abc import QueryABC, \
    _exit_on_override, \
    _exit_on_partial_query, \
    _check_and_extract_list_or_valid_typed_arguments

from athenaSQL.orders import ASC, DESC
from athenaSQL.column import Column

window_query_template = """
OVER (
    {%- if partitions -%}
    PARTITION BY
    {%- for partition in partitions -%}
    {{' '}}{{ partition }}
    {%- if not loop.last -%}
    ,
    {%- endif -%}
    {%- endfor -%}
    {%- endif -%}
    {%- if orders %}
      ORDER BY 
    {%- for order in orders -%}
    {{' '}}{{ order }}
    {%- if not loop.last -%}
    ,
    {%- endif -%}
    {%- endfor -%}
    {% endif %})
"""


class WindowQuery(QueryABC):
    """
    A query class to constract to constract the clause after and including
    over syntax in a sql window function.

    >>> win_func() OVER (PARTITION BY col1 ORDER BY col2)

    this class constract `OVER (PARTITION BY col1 ORDER BY col2)` caluse
    """

    def __init__(self):
        self._partitions = []
        self._orders = []

    def _to_sql(self):

        # check if query constraction is complete
        _exit_on_partial_query(
            (self._partitions or self._orders),
            'partitionBy', 'WindowQuery')

        return Template(window_query_template).render(
            partitions=self._partitions,
            orders=self._orders
        ).strip()

    def partitionBy(self, *cols):
        """
        """
        # check if method is already been called in an instance
        _exit_on_override(self._partitions, 'partitionBy',
                          'partitionBy(*cols)')

        arguments = _check_and_extract_list_or_valid_typed_arguments(
            cols, 'partitionBy')

        clone_obj = copy.deepcopy(self)
        clone_obj._partitions = arguments

        return clone_obj

    def orderBy(self, *cols):
        """
        """
        # check if method is already been called in an instance
        _exit_on_override(self._orders, 'orderBy', 'orderBy(*cols)')

        arguments = _check_and_extract_list_or_valid_typed_arguments(
            cols, 'orderBy', valid_types=(str, ASC, DESC))

        # should also be done in Column types
        ordering_cols = list(map(lambda arg: arg
                                 if isinstance(arg, (ASC, DESC))
                                 else Column(arg),
                                 arguments))

        clone_obj = copy.deepcopy(self)
        clone_obj._orders = ordering_cols

        return clone_obj


Window = WindowQuery()

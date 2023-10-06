import copy
from jinja2 import Template

from athenaSQL.queries.query_abc import QueryABC, \
    _exit_on_override, \
    _exit_on_uncalled_preceding_method, \
    _check_and_extract_list_or_valid_typed_arguments

from athenaSQL.orders import ASC, DESC
from athenaSQL.column import Column, ConditionalColumn, AliasColumn, \
    AggregateColumn

select_query_template = """
{% if cte_tables %}
WITH{{' '}}
{%- for name, query in cte_tables.items() -%}
{{ name }} AS (
    {{ query._to_sql()|indent(4) }}
){%- if not loop.last -%},{{' '}}{%- endif -%}
{%- endfor -%}
{% endif %}
SELECT
    {%- if columns -%}
    {%- for col in columns %}
    {{col}}
    {%- if not loop.last -%}
    ,
    {%- endif -%}
    {%- endfor -%}
    {% else %}
    *
    {%- endif %}
FROM {% if database -%}"{{database}}".{%- endif %}"{{table}}"
{% if filters -%}
WHERE
    {% for filter in filters -%}
    {{filter}}{{" "}}
    {%- if not loop.last -%}
    AND
    {% endif -%}
    {% endfor -%}
{% endif %}
{% if grouping_cols -%}
GROUP BY{{" "}}
    {%- for col in grouping_cols -%}
    {{col}}
    {%- if not loop.last -%}
    {{", "}}
    {%- endif -%}
    {%- endfor -%}
{% endif %}
{% if group_filters -%}
HAVING
    {% for filter in group_filters -%}
    {{filter}}{{" "}}
    {%- if not loop.last -%}
    AND
    {% endif -%}
    {% endfor -%}
{% endif %}
{% if ordering_cols -%}
ORDER BY{{" "}}
    {%- for col in ordering_cols -%}
    {{col}}
    {%- if not loop.last -%}
    {{", "}}
    {%- endif -%}
    {%- endfor -%}
{% endif %}
{% if limit -%}
LIMIT {{limit}}
{% endif %}
"""


class SelectQuery(QueryABC):
    def __init__(self, table, database=None):
        self._table = table
        self._database = database

        QueryABC.__init__(self, database, table)

        self._cte_tables = {}
        self._columns = []
        self._filters = []
        self._grouping_cols = []
        self._group_filters = []
        self._limit = ''
        self._ordering_cols = []

    def _to_sql(self):
        """
        """

        # exit if select is grouped and a non grouping column is not aggregated
        # select is grouped
        if self._grouping_cols:
            _unagg_cols = [col for col in self._columns
                           # stringify all because built in __eq__ has ben replaced
                           if str(col) not in list(map(str, self._grouping_cols))
                           and type(col) in (Column, str)]

            if _unagg_cols:
                raise SyntaxError(f"'{_unagg_cols[0]}' must be an aggregated "
                                  "expression or inserted in 'groupBy' method")

        return Template(select_query_template).render(
            table=self._table,
            database=self._database,
            cte_tables=self._cte_tables,
            columns=self._columns,
            filters=self._filters,
            grouping_cols=self._grouping_cols,
            group_filters=self._group_filters,
            ordering_cols=self._ordering_cols,
            limit=self._limit
        ).strip()

    def select(self, *cols):
        """
        specify columns to select 

        Parameters
        ----------
        cols: list or str
        """
        # check if select() method has already been called in an instance
        _exit_on_override(self._columns, 'selects', 'select(cols)')

        # check if argument is either a list, str or Column and return list
        # of args else raises method argument error
        arguments = _check_and_extract_list_or_valid_typed_arguments(cols,
                                                                     'select')

        columns = list(map(lambda arg: arg
                           if isinstance(arg, (Column, AliasColumn))
                           else Column(arg),
                           arguments))

        clone_obj = copy.deepcopy(self)
        clone_obj._columns = columns

        return clone_obj

    def filter(self, condition):
        """
        Add fiter condition to where clause

        Parameters
        ----------
        condition: ConditionalColumn
            a sql where caluse ConditionalColumn
        """

        if not isinstance(condition, ConditionalColumn):
            raise TypeError('.filter() can only accept a ConditionalColumn')

        clone_obj = copy.deepcopy(self)
        # update the instances filters
        clone_obj._filters.append(condition)

        return clone_obj

    def groupBy(self, *cols):
        """
        """
        # check if groupBy() method has already been called in an instance
        _exit_on_override(self._grouping_cols,
                          'grouping columns',
                          'groupBy(cols)')

        # check if argument is either a list or string and return list of args
        # else raises method argument error
        # TODO type should be of Column not instance of Column
        arguments = _check_and_extract_list_or_valid_typed_arguments(cols,
                                                                     'groupBy')

        grouping_cols = list(map(lambda arg: arg
                                 if isinstance(arg, (Column, AliasColumn))
                                 else Column(arg),
                                 arguments))

        clone_obj = copy.deepcopy(self)
        clone_obj._grouping_cols = grouping_cols

        # if select columns are not specified, add grouping cols
        if len(clone_obj._columns) == 0:
            clone_obj._columns = grouping_cols

        return clone_obj

    def filterGroup(self, condition):
        """
        """

        # exit if groupBy hasn't already been called
        _exit_on_uncalled_preceding_method(self._grouping_cols,
                                           'filterGroup',
                                           'groupBy')

        if not isinstance(condition, ConditionalColumn):
            raise TypeError(
                '.filterGroup() can only accept a ConditionalColumn')

        clone_obj = copy.deepcopy(self)
        # update group filters
        clone_obj._group_filters.append(condition)

        return clone_obj

    def agg(self, *aggs):
        """
        Accept one or more AggregateColumn and either replace or add to
        select column
        """

        # check all aggs are types of AggregateColumn
        # else raises method argument error
        arguments = _check_and_extract_list_or_valid_typed_arguments(aggs,
                                                                     'agg', valid_types=(AggregateColumn, AliasColumn))

        # if host of AliasColumn is not AggregateColumn throw exception
        for agg in aggs:
            if (isinstance(agg, AliasColumn) and
                    not isinstance(agg.host, AggregateColumn)):
                raise TypeError('`.agg()` can only accept `AggregateColumn` or'
                                ' it\'s alias.')

        # extract base column name to check cross check with column list
        agg_columns = [col.column for col in arguments]
        # remove column that are aggregated if exist
        filtered_cols = [col for col in self._columns
                         if col.column not in agg_columns]

        clone_obj = copy.deepcopy(self)
        # update columns with agg columns
        clone_obj._columns = filtered_cols + arguments

        return clone_obj

    def limit(self, limit):
        """
        Add limit to the instance query
        A limit lets you specify how many rows to return, it starts from top

        Parameter
        ---------
        limit: int or str 
            integer or numeric string
        """
        # check if limit() method has already been called in an instance
        _exit_on_override(self._limit,
                          'limit',
                          'limit(limit)')

        if not str(limit).isdigit():
            raise TypeError('.limit() can only acccept an integer '
                            'or an integer numeric string')

        clone_obj = copy.deepcopy(self)
        clone_obj._limit = str(limit)

        return clone_obj

    def orderBy(self, *cols):
        """
        Add columns to order the result by

        Parameters
        ----------
        cols: str or list
        """
        # check if orderBy() method has already been called in an instance
        _exit_on_override(self._ordering_cols,
                          'ordering columns',
                          'orderBy(cols)')

        # check if argument is either a list, ASC or DESC and return list of args
        # else raises method argument error
        arguments = _check_and_extract_list_or_valid_typed_arguments(cols,
                                                                     'orderBy', valid_types=(str, ASC, DESC))

        # should also be done in Column types
        ordering_cols = list(map(lambda arg: arg
                                 if isinstance(arg, (ASC, DESC))
                                 else Column(arg),
                             arguments))

        clone_obj = copy.deepcopy(self)
        clone_obj._ordering_cols = ordering_cols

        return clone_obj

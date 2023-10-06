import copy
from jinja2 import Template

from athenaSQL.queries.query_abc import QueryABC, \
    _exit_on_override, \
    _exit_on_partial_query, \
    _check_and_extract_list_or_valid_typed_arguments

from athenaSQL.column import Column
from athenaSQL.queries.select import SelectQuery

insert_query_template = """
INSERT INTO "{{database}}"."{{table}}"
{%- if columns -%}
(
        {%- for col in columns %}
        {{col}}
        {%- if not loop.last -%}
        ,
        {%- endif -%}
        {%- endfor %}
    )
{%- endif %}
{{select_query}}
"""


class InsertQuery(QueryABC):
    def __init__(self, database, table):
        """
        """
        self._database = database
        self._table = table

        QueryABC.__init__(self, database, table)

        self._select_query = None
        self._column_order = []

    def _to_sql(self):
        """
        """

        # required attributes
        req_att = {'insert': self._select_query}

        # check if query constraction is complete
        for method, att in req_att.items():
            _exit_on_partial_query(att, method, 'InsertQuery')

        return Template(insert_query_template).render(
            database=self._database,
            table=self._table,
            columns=self._column_order,
            select_query=self._select_query._to_sql()
        ).strip()

    def insert(self, select_query):
        """
        """
        # check if method is already been called in an instance
        _exit_on_override(self._select_query, 'insert', 'insert(SelectQuery)')

        #
        arguments = _check_and_extract_list_or_valid_typed_arguments(
            select_query,
            'insert',
            valid_types=SelectQuery)

        clone_obj = copy.deepcopy(self)
        clone_obj._select_query = arguments

        return clone_obj

    def column_order(self, *cols):
        """
        """

        #
        _exit_on_override(self._column_order, 'column_order',
                          'column_order(cols)')

        #
        arguments = _check_and_extract_list_or_valid_typed_arguments(
            cols,
            'column_order',
            valid_types=(str, Column)
        )

        # convert string to column type
        columns = list(map(lambda arg: Column(str(arg)), arguments))

        clone_obj = copy.deepcopy(self)
        clone_obj._column_order = columns

        return clone_obj

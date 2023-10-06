import copy
from jinja2 import Template

from athenaSQL.queries.query_abc import QueryABC, \
    _exit_on_override, \
    _exit_on_partial_query

from athenaSQL.queries.select import SelectQuery

create_as_query_template = """

{% macro render_type(value) -%}
{%- if value is string -%}
'{{ value }}'
{%- elif value is boolean -%}
{{ value|lower }}
{%- else -%}
{{ value }}
{%- endif -%}
{%- endmacro -%}

CREATE TABLE "{{database}}"."{{table}}"
{% if tbl_properties -%}
WITH (
    {% for key, prop in tbl_properties.items() -%}
    {%- if key in ['partitioned_by', 'bucketed_by', 'partitioning'] -%}
    {{ key }}=ARRAY[
        {%- for col in prop -%}
        '{{ col }}'
        {%- if not loop.last -%},{{' '}}{%- endif -%}
        {%- endfor -%}
    ]
    {%- else -%}
    {{ key }}={{ render_type(prop) }}
    {%- endif %}
    {%- if not loop.last -%},{%- endif %}
    {% endfor -%}
)
{% endif -%}
AS 
{{ select_query._to_sql() }}
{% if not with_data -%}
WITH NO DATA
{%- endif -%}
"""


class CreateAsQuery(QueryABC):
    """
    Creates a new table populated with the results of a SELECT query.
    https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html
    """

    def __init__(self, database, table):
        self._database = database
        self._table = table

        self._tbl_properties = {}
        self._select_query = None
        self._with_data = True

    def _to_sql(self):

        # check if query is compelete before generating sql
        req_att = {'createAs': self._select_query}
        for method, att in req_att.items():
            _exit_on_partial_query(att, method, 'CreateAsQuery')

        return Template(create_as_query_template).render(
            database=self._database,
            table=self._table,
            select_query=self._select_query,
            tbl_properties=self._tbl_properties,
            with_data=self._with_data
        ).strip()

    def withData(self, opt):
        """
        """

        _exit_on_override(self._with_data, 'withData', 'withData(bool)')

        if not isinstance(opt, bool):
            raise TypeError(f'`{type(opt).__name__}` is not of a type `bool`')

        clone_obj = copy.deepcopy(self)
        clone_obj._if_not_exists = opt

        return clone_obj

    def createAs(self, select_query):
        """
        """

        _exit_on_override(self._select_query, 'createAs',
                          'createAs(SelectQuery)')

        if not isinstance(select_query, SelectQuery):
            raise TypeError(f'`{type(select_query).__name__}` is not of a '
                            'type `SelectQuery`')

        clone_obj = copy.deepcopy(self)
        clone_obj._select_query = select_query

        return clone_obj

    def withTblProps(self, props):
        """
        """
        _exit_on_override(self._tbl_properties,
                          'table properties',
                          'withTblProp(props)')

        for k in ['partitioned_by', 'bucketed_by', 'partitioning']:
            if k in props.keys() and not isinstance(props[k], dict):
                TypeError(f'Unsupported type `{type(props[k]).__name__}` for '
                          f'`{k}`, use `dict` type instead.')

        clone_obj = copy.deepcopy(self)
        clone_obj._tbl_properties = props

        return clone_obj

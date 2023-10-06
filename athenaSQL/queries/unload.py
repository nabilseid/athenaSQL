import copy
from jinja2 import Template

from athenaSQL.queries.select import SelectQuery
from athenaSQL.queries.query_abc import QueryABC, \
    _exit_on_override, \
    _exit_on_partial_query

unload_query_template = """
{% macro render_type(value) -%}
{%- if value is string -%}
'{{ value }}'
{%- elif value is boolean -%}
{{ value|lower }}
{%- else -%}
{{ value }}
{%- endif -%}
{%- endmacro -%}

UNLOAD (
    {{ select_query._to_sql()|indent(4) }}
)
TO '{{ location }}'
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
{%- endif -%}
"""


class UnloadQuery(QueryABC):
    """
    """

    def __init__(self):
        QueryABC.__init__(self, None, None)

        self._select_query = None
        self._location = None
        self._tbl_properties = {}

    def _to_sql(self):

        # check if query is compelete before generating sql
        req_att = {'unload': self._select_query,
                   'location': self._location}
        for method, att in req_att.items():
            _exit_on_partial_query(att, method, 'UnloadQuery')

        return Template(unload_query_template).render(
            select_query=self._select_query,
            location=self._location,
            tbl_properties=self._tbl_properties
        ).strip()

    def unload(self, select_query):
        """
        """
        _exit_on_override(self._select_query, 'unload', 'unload(SelectQuery)')

        if not isinstance(select_query, SelectQuery):
            raise TypeError(f'{type(select_query).__name__} is not of a type '
                            f'SelectQuery')

        clone_obj = copy.deepcopy(self)
        clone_obj._select_query = select_query

        return clone_obj

    def location(self, location):
        """
        """
        _exit_on_override(self._location, 'location', 'location(loc)')

        # TODO validate s3 location
        clone_obj = copy.deepcopy(self)
        clone_obj._location = location

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


def unload(select_query):
    """
    """
    return UnloadQuery().unload(select_query)

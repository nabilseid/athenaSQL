import copy
from jinja2 import Template

from athenaSQL.queries.query_abc import QueryABC, \
    _exit_on_override, \
    _exit_on_partial_query, \
    _check_and_extract_list_or_valid_typed_arguments

from athenaSQL.column import Column, NewColumn

create_query_template = """

{### schema set ###}
{% set set_schema -%}
{%- if columns -%}(
{%- for col in columns %}
    {{ col }}
{%- if not loop.last -%},{%- endif -%}
{%- endfor %}
)
{%- endif -%}
{%- endset %}

{### partition by set ###}
{% set set_partition -%}
{%- if partitions -%}
PARTITIONED BY (
{%- for partition in partitions -%}
{{ partition }}
{%- if not loop.last -%},{%- endif -%}
{%- endfor -%}
)
{%- endif -%}
{%- endset %}

{### clustered by set ###}
{% set set_cluster -%}
{%- if clusters -%}
CLUSTERED BY (
{%- for cluster in clusters -%}
{{ cluster }}
{%- if not loop.last -%},{%- endif -%}
{%- endfor -%}
) INTO {{ buckets }} BUCKETS
{%- endif -%}
{%- endset %}

{### row format set ###}
{% set set_row_format -%}
{%- if row_format -%}
ROW FORMAT SERDE '{{ row_format }}'
{%- endif -%}
{%- endset %}

{### store format set ###}
{% set set_store_format -%}
{%- if store_format -%}
STORED AS {{ store_format }}
{%- elif input_format and output_format -%}
STORED AS INPUTFORMAT
    '{{ input_format }}'
OUTPUTFORMAT
    '{{ output_format }}'
{%- endif -%}
{%- endset %}

{### serde properties set ###}
{% set set_serde_properties -%}
{%- if serde_properties -%}
WITH SERDEPROPERTIES (
    {%- for key, prop in serde_properties.items() %}
    "{{ key }}"="{{ prop }}"
    {%- if not loop.last -%},{%- endif -%}
    {%- endfor %}
)
{%- endif -%}
{%- endset %}

{### location set ###}
{% set set_location -%}
{%- if location -%}
LOCATION '{{ location }}'
{%- endif -%}
{%- endset %}

{### table properties set ###}
{% set set_tbl_properties -%}
{%- if tbl_properties -%}
TBLPROPERTIES (
    {%- for key, prop in tbl_properties.items() %}
    "{{ key }}"="{{ prop }}"
    {%- if not loop.last -%},{%- endif -%}
    {%- endfor %}
)
{%- endif -%}
{%- endset %}

CREATE EXTERNAL TABLE {% if if_not_exists -%}IF NOT EXISTS{%- endif %}
"{{database}}"."{{table}}" {{ set_schema }}
{{ set_partition }}
{{ set_cluster }}
{{ set_row_format }}
{{ set_store_format }}
{{ set_serde_properties }}
{{ set_location }}
{{ set_tbl_properties }}
"""


class CreateQuery(QueryABC):
    def __init__(self, database, table):
        """
        """
        self._database = database
        self._table = table

        QueryABC.__init__(self, database, table)

        self._if_not_exists = True
        self._columns = []
        self._partition_by = []
        self._clustered_by = []
        self._buckets = None
        self._row_format = None
        self._file_format = None
        self._input_format = None
        self._output_format = None
        self._serde_properties = {}
        self._location = None
        self._tbl_properties = {}

    def _to_sql(self):

        # check if query is compelete before generating sql
        req_att = {'columns': self._columns}
        for method, att in req_att.items():
            _exit_on_partial_query(att, method, 'CreateQuery')

        return Template(create_query_template).render(
            database=self._database,
            table=self._table,
            if_not_exists=self._if_not_exists,
            columns=self._columns,
            partitions=self._partition_by,
            clusters=self._clustered_by,
            buckets=self._buckets,
            row_format=self._row_format,
            store_format=self._file_format,
            input_format=self._input_format,
            output_format=self._output_format,
            serde_properties=self._serde_properties,
            location=self._location,
            tbl_properties=self._tbl_properties
        ).strip()

    def ifNotExists(self, opt):
        """
        """

        _exit_on_override(self._if_not_exists,
                          'ifNotExists', 'ifNotExists(bool)')

        if not isinstance(opt, bool):
            raise TypeError(f'`{type(opt).__name__}` is not of a type `bool`')

        clone_obj = copy.deepcopy(self)
        clone_obj._if_not_exists = opt

        return clone_obj

    def columns(self, *new_columns):
        """
        """

        _exit_on_override(self._columns, 'columns', 'columns(new_columns)')

        arguments = _check_and_extract_list_or_valid_typed_arguments(
            new_columns,
            'columns',
            valid_types=(NewColumn)
        )
        clone_obj = copy.deepcopy(self)
        clone_obj._columns = arguments

        return clone_obj

    def partitionBy(self, *cols):
        """
        """
        _exit_on_override(self._partition_by,
                          'partitionBy',
                          'partitionBy(cols)')

        arguments = _check_and_extract_list_or_valid_typed_arguments(
            cols,
            'partitionBy',
            valid_types=(str, Column)
        )

        columns = list(map(lambda arg: Column(str(arg)), arguments))

        clone_obj = copy.deepcopy(self)
        clone_obj._partition_by = columns

        return clone_obj

    def clusterBy(self, *cols, buckets=None):
        """
        """
        _exit_on_override(self._clustered_by,
                          'clusterBy',
                          'clusterBy(*cols, buckets)')

        arguments = _check_and_extract_list_or_valid_typed_arguments(
            cols,
            'clusterBy',
            valid_types=(str, Column)
        )

        if not isinstance(buckets, int):
            raise TypeError(f'{buckets} is not a type of an int')

        columns = list(map(lambda arg: Column(str(arg)), arguments))

        clone_obj = copy.deepcopy(self)
        clone_obj._clustered_by = columns
        clone_obj._buckets = buckets

        return clone_obj

    def row_format(self, lib_name):
        """
        https://docs.aws.amazon.com/athena/latest/ug/serde-about.html
        ROW FORMAT DELIMITED - is not supported
        ROW FORMAT SERDE - supported
        """

        _exit_on_override(self._row_format,
                          'row_format',
                          'row_format(lib_name)')

        clone_obj = copy.deepcopy(self)
        clone_obj._row_format = lib_name

        return clone_obj

    def stored_as(self, file_format):
        """
        Specifies the file format for table data. 
        If omitted, TEXTFILE is the default. Options for file_format are:
        SEQUENCEFILE, TEXTFILE, RCFILE, ORC, PARQUET, AVRO, ION
        """
        _exit_on_override((self._file_format or (self._input_format and self._output_format)),
                          'storage format',
                          'stored_as() or stored_as_io()')

        supported_formats = ['SEQUENCEFILE', 'TEXTFILE', 'RCFILE',
                             'ORC', 'PARQUET', 'AVRO', 'ION']
        if file_format.upper() not in supported_formats:
            raise TypeError(f'`{file_format}` is not a supported file format. '
                            f'Supported formats: {supported_formats}')

        clone_obj = copy.deepcopy(self)
        clone_obj._file_format = file_format.upper()

        return clone_obj

    def stored_as_io(self, *, input_format, output_format):
        """
        INPUTFORMAT input_format_classname OUTPUTFORMAT output_format_classname
        """
        _exit_on_override((self._file_format or (self._input_format and self._output_format)),
                          'storage format',
                          'stored_as() or stored_as_io()')

        clone_obj = copy.deepcopy(self)
        clone_obj._input_format = input_format
        clone_obj._output_format = output_format

        return clone_obj

    def serde_properties(self, serdeprops):
        """
        """
        _exit_on_override(self._serde_properties,
                          'serde properties',
                          'serde_properties(serdeprops)')

        clone_obj = copy.deepcopy(self)
        clone_obj._serde_properties = serdeprops

        return clone_obj

    def location(self, loc):
        """
        """
        _exit_on_override(self._location,
                          'location',
                          'location(loc)')

        # TODO check if loc is a valid s3 path

        clone_obj = copy.deepcopy(self)
        clone_obj._location = loc

        return clone_obj

    def tbl_properties(self, props):
        """
        """
        _exit_on_override(self._tbl_properties,
                          'table properties',
                          'tbl_properties(props)')

        clone_obj = copy.deepcopy(self)
        clone_obj._tbl_properties = props

        return clone_obj

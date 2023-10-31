CREATE
======

**Synopsis**

::

    table.create()
        [.ifNotExists(bool)]
        [.columns(new_column [,...])]
        [.partitionBy(column [,...])]
        [.clusterBy(column [,...])]
        [.location(location)]
        [.row_format(row_format)]
        [.stored_as(file_format)]
        [.stored_as_io(input_format, output_format)]
        [.serde_properties({"property":"value" [,...]})]
        [.tbl_properties({"property":"value" [,...]})]
        [.exec()]

Create an external table. Unlike ``CREATE AS`` table schema and
properties must be specified.

The column type passed when creating a table is special type of column(
``NewColumn`` ) that accepts column name and data type. NewColumn can be
created using ``nCol()`` function.

``row_format()`` only supports SERDE. ROW FORMAT DELIMITED is not
supported. Since DELIMITED is not supported properties have to be
specified through ``serde_properties()``. `More
ref <https://docs.aws.amazon.com/athena/latest/ug/serde-about.html>`__

``stored_as_io()`` is an alternative for ``stored_as()`` that accept
input and output format. Both methods cannot be called on a single
instance simultaneously. ``stored_as()`` accept single row format. These
row formats can be imported from ``athenaSQL.column_type.COLUMNTYPE``.
Supported row formats: SEQUENCEFILE, TEXTFILE, RCFILE, ORC, PARQUET,
AVRO, ION. By default TEXTFILE is selected.

**Using CREATE**

.. code:: python

    from athenaSQL.functions import F
    from athenaSQL.column_type import COLUMNTYPE

    # create abstract representation of the table
    new_table = AthenaTable(database='db_name', table='new_table')

    create_table_query = (new_table.create()
                             .columns(
                                 nCol('col1', COLUMNTYPE.string),
                                 nCol('col2', COLUMNTYPE.char(4)),
                                 nCol('col3', COLUMNTYPE.int))
                             .partitionBy('col3')
                             .clusterBy('col2', buckets=4)
                             .row_format('org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe')
                             .stored_as_io(
                                 input_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                                 output_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat')
                             .location('s3://S3-bucket-location')
                             .tbl_properties({
                                 "bucketing_format":"spark",
                                 "parquet.compression":"SNAPPY"}))

    create_table_query.show_query()

::

    CREATE EXTERNAL TABLE IF NOT EXISTS
    "db_name"."new_table" (
        `col1` string,
        `col2` char(4)
    )
    PARTITIONED BY (col1,col2)
    CLUSTERED BY (col2) INTO 4 BUCKETS
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION 's3://S3-bucket-location/'
    TBLPROPERTIES (
        "bucketing_format"="spark",
        "parquet.compression"="SNAPPY"
    )
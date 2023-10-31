UNLOAD
======

**Synopsis**

::

    unload(select_query)
        .location(location)
        [.withTblProps({"property":"value" [,...]})]

``UNLOAD`` lets you dump a select query result to specified location.
This query is not directly callable on table instance, it is a wrapper
query for ``SELECT`` query.
`Doc <https://docs.aws.amazon.com/athena/latest/ug/unload.html>`__

**Using UNLOAD**

.. code:: python

    from athenaSQL.queries import unload

    # we can use the cta_query as an input
    unload_query = (unload(cta_query)
                        .location('s3://S3-bucket-name/sub-location/')
                        .withTblProps({"format":"PARQUET", "compression":"gzip"}))

    unload_query.show_query()

::

    UNLOAD (
        WITH demos AS (
            SELECT
                demo_id,
                demo_name,
                demo_addr
            FROM "db_name"."demo"
        ), demo_in_addis AS (
            SELECT
                demo_id,
                demo_name,
                demo_addr
            FROM "demos"
            WHERE
                demo_addr = 'addis'
        )
        SELECT
            *
        FROM "demo_in_addis"
    )
    TO 's3://S3-bucket-name/sub-location/'
    WITH (
        format='PARQUET',
        compression='gzip'
        )
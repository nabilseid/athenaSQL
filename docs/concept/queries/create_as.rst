CREATE AS
=========

**Synopsis**

::

   table.createAs(select_query)
       [.withData(bool)]
       [.withTblProps({"property":"value" [,...]})]

Create a table from a dataset that is a result of a select query. You
can use ``.withTblProps()`` to specify table properties. `CREATE AS
Doc <https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html>`__

**Using CREATE AS**

.. code:: python

    # let's use select_salary query from SELECT query section

    salary_table = Athena('employee_db').table('salary')

    create_salary_table = salary_table.createAs(select_salary).withTblProps({
                                "format":"AVRO",
                                "is_external":False,
                                "vacuum_max_snapshot_age_ms":259200,
                                "partitioning":['country', 'city']})

    create_salary_table.show_query()

::

    CREATE TABLE "employee_db"."salary"
    WITH (
        format='AVRO',
        is_external=false,
        vacuum_max_snapshot_age_ms=259200,
        partitioning=ARRAY['country', 'city']
        )
    AS
    SELECT
        city,
        AVG(age) AS age_avg,
        AVG(salary) AS salary_avg,
        MIN(salary) AS salary_min,
        MAX(salary) AS salary_max,
        MIN(country) AS country
    FROM "db_name"."table_name"
    WHERE
        age > 10 AND
        country = 'Ethiopia'
    GROUP BY city
    HAVING
        AVG(salary) > 15000
    ORDER BY salary_avg DESC
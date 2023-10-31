CTE
===

**Synopsis**

::

    withTable(cte_name, select_query)
        [.withTable(cte_name, (table|temp)select_query) [,...]]
        .table(table_name)
        .select([column, ...])
        [.filter(condition [,...])]
        [.groupBy(column [,...])]
        [.filterGroup(condition [,...])]
        [.agg(aggregation [,...])]
        [.limit(limit)]
        [.orderBy(column [,...])]
        [.exec()]

CTE is a type of select query that let’s you create a temporary tables
instead of using sub-queries. CTE can be used as an input where ever a
select query can.

Temporary tables are created using ``.withTable()`` method, it accept
name for temporary table and a select query. The first select query that
is passed to CTE must be a select statement on a table. Subsucent CTEs
can be constracted from a select statement on a table or a temporary
table(another CTE table).

CTE is a type of select query thus it should be finished with a select
statment. After creating the temporary tables a table name should be
selected for the final select statement. Selecting a table name returns
an abstract table for the selected table name with only ``.select()``
query capability.

**Using CTE**

.. code:: python

    from athenaSQL import TempTable
    from athenaSQL.queries import withTable
    from athenaSQL.functions import col

    table_demo = AthenaTable(database='db_name', table='demo')
    select_demo = table_demo.select('demo_id', 'demo_name', 'demo_addr')

    select_demo_in_addis = (TempTable('demos')
                                .select('demo_id', 'demo_name', 'demo_addr')
                                .filter(col('demo_addr') == 'addis'))

    cta_query = (withTable('demos', select_demo)
                    .withTable('demo_in_addis', select_demo_in_addis)
                    .table('demo_in_addis')
                    .select())

    cta_query.show_query()

::

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
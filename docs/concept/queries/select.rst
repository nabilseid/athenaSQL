SELECT
======

**Synopsis**

::

    table.select([column, ...])
        [.filter(condition [,...])]
        [.groupBy(column [,...])]
        [.filterGroup(condition [,...])]
        [.agg(aggregation [,...])]
        [.limit(limit)]
        [.orderBy(column [,...])]
        [.exec()]

If no column is provided all columns will selected. ``.filter()``,
``.filterGroup()``, ``.agg()`` can be chained more than once.

A column can be represent by a string. To perform arithmetic, comparison
and logical operation on a column, a Column type must be use. To read
more on column refer to Column section

**Creating a column instance**

.. code:: python

    import athenaSQL.functions as F

    # create a new column instance. we can use F.col or F.column
    col = F.col('col_name')
    col2 = F.column('col_2')

    # we can perform arithmeti, comparison, logical operation on columns
    col_multi = col * 10
    col_compare = col >= 10
    col_logical = (col >= 10 and col2 <= 15)

SELECT Chain Method Conditions
------------------------------

-  ``.filterGroup()`` can only be applied to grouped select query.
-  If no column is provided on a grouped selecte query only grouping
   columns will be selected.
-  In grouped select query non-grouping columns must be aggregated.
-  Aggregating already selected column will replace it unless once of
   them is aliased.
-  Alternative to using ``.agg()`` is to directly add aggregated columns
   to ``.select()``.

**Using SELECT Query**

.. code:: python

    # let's use the table instance from `AthenaTable` example
    table.select().show_query()

::

    SELECT
        *
    FROM "db_name"."table_name"

Comprehensive SELECT Query
--------------------------

.. code:: python

    import athenaSQL.functions as F
    from athenaSQL.orders import ASC, DESC

    select_salary = (table.select('city', 'country')
                        .filter(F.col('age') > 10)
                        .filter(F.col('country') == 'Togo')
                        .groupBy('city')
                        .filterGroup(F.avg(F.col('salary')) > 15000)
                        .agg(F.avg(F.col('age')).alias('age_avg'))
                        .agg(F.avg(F.col('salary')).alias('salary_avg'))
                        .agg(F.min(F.col('salary')).alias('salary_min'))
                        .agg(F.max(F.col('salary')).alias('salary_max'))
                        .agg(F.min(F.col('country')).alias('country'))
                        .orderBy(ASC('salary_avg')))

    select_salary.show_query()

::
    
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
        country = 'Togo'
    GROUP BY city
    HAVING
        AVG(salary) > 15000
    ORDER BY salary_avg DESC
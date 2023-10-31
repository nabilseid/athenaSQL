WINDOW
======

**Synopsis**

::

    window_function([operand, [offset, [default]]])
        .over(Window
                .partitionBy(column, [,...])
                [.orderBy(Column, [,...])]
        )
        [.alias(alias)]

Window functions are imported and called as a python function. The
``over`` clause is passed as a Window query. Aggregation function can be
used in place of window functions. Lets consider the below window
function.

``NTILE(4) OVER (PARTITION BY campaignId ORDER BY flight) AS quartile``

``NTILE`` is the function everything after the ``OVER`` clause is Window
query.

.. code:: python

    from athenaSQL.queries.window import Window
    from athenaSQL.functions.window import ntile

    window_col = ntile(4).over(
                    Window
                        .partitionBy('campaignId')
                        .orderBy('flight')
                ).alias('quartile')

    print(window_col._sql_clause)

::
    
    NTILE(4) OVER (PARTITION BY campaignId ORDER BY flight) AS quartile

**Using WINDOW**

.. code:: python

    from athenaSQL.queries.window import Window
    from athenaSQL.functions.window import row_number
    import athenaSQL.functions AS F

    # lets use salary table
    sum_window = Window.partitionBy('city', 'country').orderBy('salary_avg')
    row_window = Window.orderBy('salary_avg')

    select_win_query = salary_table.select(
                            'country',
                            'city',
                            'salary',
                            row_number().over(row_window).alias('row_number'),
                            F.sum(F.col('salary')).over(sum_window).alias('cumulative_salary')
                        )

    select_win_query.show_query()

::

    SELECT
        country,
        city,
        salary,
        row_number() OVER (
            ORDER BY salary_avg) AS row_number,
        SUM(salary) OVER (PARTITION BY city, country
            ORDER BY salary_avg) AS cumulative_salary
    FROM "employee_db"."salary"
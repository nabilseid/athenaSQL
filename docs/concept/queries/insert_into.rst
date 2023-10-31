INSERT INTO
===========

**Synopsis**

::

    table.insert(select_query)
        [.column_order(column, [,...])]

Insert query accepts a select query instance to insert selected dataset
into a table.

``.column_order()`` is an optional method to specify and order what
column from the dataset to insert.

**Using INSERT INTO**

.. code:: python

    # the select_salary select query is used as a dataset from above

    # constract salary table from employee_db database
    salary_table = Athena('employee_db').table('salary')

    # constract insert salary dataset query
    insert_salary_query = salary_table.insert(select_salary)

    insert_salary_query.show_query()

::

    INSERT INTO "employee_db"."salary"
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

Ordered Column Insert
---------------------

.. code:: python
    
    # specified order column to insert from salary dataset
    insert_salary_query_ordered = (salary_table.insert(select_salary)
                                    .column_order('country', 'city', 'age_avg',
                                                  'salary_avg'))    

    insert_salary_query_ordered.show_query()

::

    INSERT INTO "employee_db"."salary"(
            country,
            city,
            age_avg,
            salary_avg
        )
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
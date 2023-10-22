# adflowSQL

adflowSQL is an Adflow module to build Athena query using python language. It is inspired by sparks sql module. It borrow some sparkSQL's concept [sparkSQL](https://spark.apache.org/docs/preview/api/python/_modules/index.html).

It has 4 main components: `Database & Table`, `Queries`, `Functions`, `Columns`.

TODO typs: `Column type`, `Order type`

## Database & Tables

adflowSQL uses `Athena` class to abstracted athena databases and `AthenaTable` for athena tables. CTE temporary tables are abstracted by `TempTable`.

`AthenaTable` cannot be constracted without a database. `TempTable` on the other hand doesn't have database.

> All queries are performed on `AthenaTable` instance.
>
> Only `SELECT` query is availabile for CTE table. To use CTE with other queries pass it as a select query argument.

**Using AthenaTable**

```python
from athenaSQL import Athena, AthenaTable, TempTable

# creating athena table instance from database
table = Athena('db_name').table('table_name')

# creating athena table instance directly
table = AthenaTable('db_name', 'table_name')

# creating temp table instance
temp_table = TempTable('temp_tbl_name')
```

## Queries

All queries need either `AthenaTable` or `TempTable` to execute. Query instance has `show_query()` that let's you preview the sql query constracted and `exec()` to execute constracted query on Athena, `exec()` returns a dataframe.

Supported queries: `SELECT`, `INSERT INTO`, `CREATE`, `CREATE AS`, `CTE`, `UNLOAD`, `WINDOW`

### SELECT Query

**Synopsis**

```
table.select([column, ...])
    [.filter(condition [,...])]
    [.groupBy(column [,...])]
    [.filterGroup(condition [,...])]
    [.agg(aggregation [,...])]
    [.limit(limit)]
    [.orderBy(column [,...])]
    [.exec()]
```

If no column is provided all columns will selected. `.filter()`, `.filterGroup()`, `.agg()` can be chained more than once.

A column can be represent by a string. To perform arithmetic, comparison and logical operation on a column, a Column type must be use. To read more on column refer to Column section

**Creating a column instance**

```python
import athenaSQL.functions as F

# create a new column instance. we can use F.col or F.column
col = F.col('col_name')
col2 = F.column('col_2')

# we can perform arithmeti, comparison, logical operation on columns
col_multi = col * 10
col_compare = col >= 10
col_logical = (col >= 10 and col2 <= 15)
```

**SELECT chain method conditions**

- `.filterGroup()` can only be applied to grouped select query.
- If no column is provided on a grouped selecte query only grouping columns will be selected.
- In grouped select query non-grouping columns must be aggregated.
- Aggregating already selected column will replace it unless once of them is aliased.
- Alternative to using `.agg()` is to directly add aggregated columns to `.select()`.

**Using SELECT**

```python
# let's use the table instance from `AthenaTable` example
table.select().show_query()

# SELECT
#     *
# FROM "db_name"."table_name"


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

# SELECT
#     city,
#     AVG(age) AS age_avg,
#     AVG(salary) AS salary_avg,
#     MIN(salary) AS salary_min,
#     MAX(salary) AS salary_max,
#     MIN(country) AS country
# FROM "db_name"."table_name"
# WHERE
#     age > 10 AND
#     country = 'Togo'
# GROUP BY city
# HAVING
#     AVG(salary) > 15000
# ORDER BY salary_avg DESC
```

## INSERT INTO Query

**Synopsis**

```
table.insert(select_query)
    [.column_order(column, [,...])]
```

Insert query accepts a select query instance to insert selected dataset into a table.

`.column_order()` is an optional method to specify and order what column from the dataset to insert.

**Using INSERT INTO**

```python
# the select_salary select query is used as a dataset from above

# constract salary table from employee_db database
salary_table = Athena('employee_db').table('salary')

# constract insert salary dataset query
insert_salary_query = salary_table.insert(select_salary)

insert_salary_query.show_query()

#INSERT INTO "employee_db"."salary"
#SELECT
#    city,
#    AVG(age) AS age_avg,
#    AVG(salary) AS salary_avg,
#    MIN(salary) AS salary_min,
#    MAX(salary) AS salary_max,
#    MIN(country) AS country
#FROM "db_name"."table_name"
#WHERE
#    age > 10 AND
#    country = 'Ethiopia'
#GROUP BY city
#HAVING
#    AVG(salary) > 15000
#ORDER BY salary_avg DESC

# specified order column to insert from salary dataset
insert_salary_query_ordered = (salary_table.insert(select_salary)
                                .column_order('country', 'city', 'age_avg',
                                              'salary_avg'))

insert_salary_query_ordered.show_query()

#INSERT INTO "employee_db"."salary"(
#        country,
#        city,
#        age_avg,
#        salary_avg
#    )
#SELECT
#    city,
#    AVG(age) AS age_avg,
#    AVG(salary) AS salary_avg,
#    MIN(salary) AS salary_min,
#    MAX(salary) AS salary_max,
#    MIN(country) AS country
#FROM "db_name"."table_name"
#WHERE
#    age > 10 AND
#    country = 'Ethiopia'
#GROUP BY city
#HAVING
#    AVG(salary) > 15000
#ORDER BY salary_avg DESC
```

## CREATE Query

**Synopsis**

```
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
```

Create an external table. Unlike `CREATE AS` table schema and properties must be specified.

The column type passed when creating a table is special type of column( `NewColumn` ) that accepts column name and data type. NewColumn can be created using `nCol()` function.

`row_format()` only supports SERDE. ROW FORMAT DELIMITED is not supported. Since DELIMITED is not supported properties have to be specified through `serde_properties()`. [More ref](https://docs.aws.amazon.com/athena/latest/ug/serde-about.html)

`stored_as_io()` is an alternative for `stored_as()` that accept input and output format. Both methods cannot be called on a single instance simultaneously. `stored_as()` accept single row format. These row formats can be imported from `athenaSQL.column_type.COLUMNTYPE`. Supported row formats: SEQUENCEFILE, TEXTFILE, RCFILE, ORC, PARQUET, AVRO, ION. By default TEXTFILE is selected.

**Using CREATE**

```python
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

# CREATE EXTERNAL TABLE IF NOT EXISTS
# "db_name"."new_table" (
#     `col1` string,
#     `col2` char(4)
# )
# PARTITIONED BY (col1,col2)
# CLUSTERED BY (col2) INTO 4 BUCKETS
# ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
# STORED AS INPUTFORMAT
#     'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
# OUTPUTFORMAT
#     'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
#
# LOCATION 's3://S3-bucket-location/'
# TBLPROPERTIES (
#     "bucketing_format"="spark",
#     "parquet.compression"="SNAPPY"
# )
```

## CREATE AS Query

**Synopsis**

```
table.createAs(select_query)
    [.withData(bool)]
    [.withTblProps({"property":"value" [,...]})]
```

Create a table from a dataset that is a result of a select query. You can use `.withTblProps()` to specify table properties. [CREATE AS Doc](https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html)

**Using CREATE AS**

```python
# let's use select_salary query from SELECT query section

salary_table = Athena('employee_db').table('salary')

create_salary_table = salary_table.createAs(select_salary).withTblProps({
                          "format":"AVRO",
                          "is_external":False,
                          "vacuum_max_snapshot_age_ms":259200,
                          "partitioning":['country', 'city']})

create_salary_table.show_query()

# CREATE TABLE "employee_db"."salary"
# WITH (
#     format='AVRO',
#     is_external=false,
#     vacuum_max_snapshot_age_ms=259200,
#     partitioning=ARRAY['country', 'city']
#     )
# AS
# SELECT
#     city,
#     AVG(age) AS age_avg,
#     AVG(salary) AS salary_avg,
#     MIN(salary) AS salary_min,
#     MAX(salary) AS salary_max,
#     MIN(country) AS country
# FROM "db_name"."table_name"
# WHERE
#     age > 10 AND
#     country = 'Ethiopia'
# GROUP BY city
# HAVING
#     AVG(salary) > 15000
# ORDER BY salary_avg DESC
```

## CTE Query

**Synopsis**

```
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
```

CTE is a type of select query that let's you create a temporary tables instead of using sub-queries. CTE can be used as an input where ever a select query can.

Temporary tables are created using `.withTable()` method, it accept name for temporary table and a select query. The first select query that is passed to CTE must be a select statement on a table. Subsucent CTEs can be constracted from a select statement on a table or a temporary table(another CTE table).

CTE is a type of select query thus it should be finished with a select statment. After creating the temporary tables a table name should be selected for the final select statement. Selecting a table name returns an abstract table for the selected table name with only `.select()` query capability.

**Using CTE**

```python
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

# WITH demos AS (
#     SELECT
#         demo_id,
#         demo_name,
#         demo_addr
#     FROM "db_name"."demo"
# ), demo_in_addis AS (
#     SELECT
#         demo_id,
#         demo_name,
#         demo_addr
#     FROM "demos"
#     WHERE
#         demo_addr = 'addis'
# )
# SELECT
#     *
# FROM "demo_in_addis"
```

## UNLOAD Query

**Synopsis**

```
unload(select_query)
    .location(location)
    [.withTblProps({"property":"value" [,...]})]
```

`UNLOAD` lets you dump a select query result to specified location. This query is not directly callable on table instance, it is a wrapper query for `SELECT` query. [Doc](https://docs.aws.amazon.com/athena/latest/ug/unload.html)

**Using UNLOAD**

```python
from athenaSQL.queries import unload

# we can use the cta_query as an input
unload_query = (unload(cta_query)
                    .location('s3://S3-bucket-name/sub-location/')
                    .withTblProps({"format":"PARQUET", "compression":"gzip"}))

unload_query.show_query()

# UNLOAD (
#     WITH demos AS (
#         SELECT
#             demo_id,
#             demo_name,
#             demo_addr
#         FROM "db_name"."demo"
#     ), demo_in_addis AS (
#         SELECT
#             demo_id,
#             demo_name,
#             demo_addr
#         FROM "demos"
#         WHERE
#             demo_addr = 'addis'
#     )
#     SELECT
#         *
#     FROM "demo_in_addis"
# )
# TO 's3://S3-bucket-name/sub-location/'
# WITH (
#     format='PARQUET',
#     compression='gzip'
#     )
```

## WINDOW Query

**Synopsis**

```
window_function([operand, [offset, [default]]])
    .over(
        Window
            .partitionBy(column, [,...])
            [.orderBy(Column, [,...])]
    )
    [.alias(alias)]
```

Window functions are imported and called as a python function. The `over` clause is passed as a Window query. Aggregation function can be used in place of window functions. Lets consider the below window function.

`NTILE(4) OVER (PARTITION BY campaignId ORDER BY flight) AS quartile`

`NTILE` is the function everything after the `OVER` clause is Window query.

```python
from athenaSQL.queries.window import Window
from athenaSQL.functions.window import ntile

window_col = ntile(4).over(
                Window
                    .partitionBy('campaignId')
                    .orderBy('flight')
            ).alias('quartile')

print(window_col._sql_clause)

# NTILE(4) OVER (PARTITION BY campaignId ORDER BY flight) AS quartile
```

**Using WINDOW**

```python
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
                       F.sum(F.col('salary')).over(sum_window).alias('cumulative_salary'))

select_win_query.show_query()

# SELECT
#     country,
#     city,
#     salary,
#     row_number() OVER (
#       ORDER BY salary_avg) AS row_number,
#     SUM(salary) OVER (PARTITION BY city, country
#       ORDER BY salary_avg) AS cumulative_salary
# FROM "employee_db"."salary"
```

## Columns

Column type is used to represent a single column. It is constracted just by passing the column name as an argument. Column type are needed to support python built-in operation, to identify what type of column has passed and to constract column level queries like window functions and aliasing.

There are different kinds of columns. We are only gonna use two of them. There rest are constracted behind the scene. A plane column and new column.

Plane column is a column type with only a column name, we need this when we want to do comparison, arithmetic and logical operations on a column otherwise we can use the column name instead. We can use either `col()` or `column()` functions to create plane column.

**Using `col()`**

```python
import athenaSQL.functions as F

salary_table.select('city',                    # using column name
                    F.col('country'),          # using col()
                    F.max('salary_avg'),       # passing column name to function
                    F.max(F.col('salary_avg')) # passing column to function
                    ).show_query()

# SELECT
#     city,
#     country,
#     MAX(salary_avg),
#     MAX(salary_avg)
# FROM "employee_db"."salary"
```

New column is used when creating an EXTERNAL table. It requires both column name and data type to create new column. We can use `nCol()` function to create new column.

Data types are imported from `athenaSQL.column_type`. Supported data types are

- `boolean`
- `tinyint`
- `smallint`
- `int`
- `bigint`
- `double`
- `float`
- `string`
- `binary`
- `date`
- `timestamp`
- `char [ (length) ]`
- `varchar [ (length) ]`
- `decimal [ (precision, scale) ]`

[More reference](https://docs.aws.amazon.com/athena/latest/ug/create-table.html)

**Using `nCol()`**

```python
import athenaSQL.functions as F
from athenaSQL.column_type import COLUMNTYPE

product_table = AthenaTable(database='product_db', table='product')

(product_table.create()
    .columns(
        F.nCol('product_id', COLUMNTYPE.int),
        F.nCol('product_name', COLUMNTYPE.varchar(50)),
        F.nCol('product_detail', COLUMNTYPE.string))
    .show_query()
)

# "product_db"."product" (
#     `product_id` int,
#     `product_name` varchar(50),
#     `product_detail` string
# )
```

### Comparison, Arithmetic and Logical operations on a column

comparison, arithmetic and logical operations can be done using python's built-in operators on a column wrapped with a column type.

Supported operations

- Comparison: `<`, `<=`, `=`, `>`, `>=`, `!=`
- Arithmetic: `+`, `-`, `*`, `/`, `%`, `pow()`, `abs()`
- Logical: `&`, `|`, `~`

All logical operations should be enclosed by parenthesis. Parenthesis has a higher precedence than all supported operations.

Reverese operation is supported on all comparison, all arithmetic except `abs()` and all logical except `~`.

**Using column operations**

```python
### Comparison ###

print(F.col('age') >= 10)
print(F.col('lang') !=  'python')
print(F.max('age') < 50)
print(50 < F.max('age')) # reverse '<'


# age >= 10
# lang <> 'python'
# MAX(age) < 50
# MAX(age) > 50 -- reverse '<'


### Arithmetic ###

print(F.col('revenue') - F.col('cost'))
print((F.col('revenue') /  5).alias('rev_5'))
print(pow(F.max('age'), 4))
print(abs(F.col('cost') - 5000))
print((1 - F.col('profite_fraction')).alias('cost_fraction')) # reverse '-'


# revenue - cost
# revenue / 5 AS rev_5
# POWER(MAX(age), 4)
# ABS(cost - 5000)
# 1 - profite_fraction AS cost_fraction -- reverse '-'


### Logical ###

print(~(F.col('lang') ==  'python'))
print((F.col('age') > 10) & (F.col('age') < 20))
print(((F.col('age') > 10) & (F.col('age') < 20)) | ~(F.col('is_infant') == True))


# NOT(lang = 'python')
# (age > 10 AND age < 20)
# ((age > 10 AND age < 20) OR NOT(is_infant = True))
```

In addition to the column types mentioned there are other types which are used to identify different columns.

- `CaseColumn`: return type from a case function
- `AliasColumn`: an aliased column
- `WindowColumn`: a column with window function
- `AggregateColumn`: a column wrapped with an aggregation function
- `FunctionalColumn`: a column wrapped with a function
- `ConditionalColumn`: column with a logical operation

## Functions

Functions are performed on columns. You can get more references [here](https://trino.io/docs/current/functions.html).

Aggregating functions should be used on non grouping cols if query is grouped. Aggregating functions can also be used in place of window functions.

`sqrt(col)`: Computes the square root of the specified float value.

`abs(col)`: Computes the absolute value.

`mean(col)`: Aggregate function: returns the average of the values in a group.

`geometric_mean(col)`: Returns the geometric mean of all input values.

`stddev(col)`: Returns the sample standard deviation of all input values.

`variance(col)`: Returns the sample variance of all input values.

### Aggregation functions

`any_value(col)`: Returns an arbitrary non-null value x, if one exists.

`arbitrary(col)`: Returns an arbitrary non-null value of x, if one exists. Identical to `any_value()`.

`array_agg(col)`: Returns an array created from the input x elements.

`avg(col)`: Returns the average (arithmetic mean) of all input values.

`bool_and(col)`: Returns TRUE if every input value is TRUE, otherwise FALSE.

`bool_or(col)`: Returns TRUE if any input value is TRUE, otherwise FALSE.

`checksum(col)`: Returns an order-insensitive checksum of the given values.

`count(col)`: Returns the number of non-null input values.

`count_if(col)`: Returns the number of TRUE input values. This function is equivalent to count(CASE WHEN x THEN 1 END).

`every(col)`: This is an alias for `bool_and()`.

`geometric_mean(col)`: Returns the geometric mean of all input values.

`max(col)`: Returns the maximum value of all input values.

`min(col)`: Returns the minimum value of all input values.

`sum(col)`: Returns the sum of all input values.

### Window Functions

`cume_dist()`: Returns the cumulative distribution of a value in a group of values.

`dense_rank()`: Returns the rank of a value in a group of values.

`percent_rank()`: Returns the percentage ranking of a value in group of values.

`rank()`: Returns the rank of a value in a group of values.

`row_number()`: Returns a unique sequential number for each row.

`ntile(n)`: Divides the rows for each window partition into n buckets ranging from 1 to at most n.

`first_value(x)`: Returns the first value of the window.

`last_value(x)`: Returns the last value of the window.

`nth_value(x, offset)`: Returns the value at the specified offset from the beginning of the window.

`lead(x, [offset, [default_value]])`: Returns the value at offset rows after the current row in the window partition.

`lag(x, [offset, [default_value]])`: Returns the value at offset rows before the current row in the window partition.

### String Functions

`chr(col)`: Returns the Unicode code point n as a single character string.

`codepoint(col)`: Returns the Unicode code point of the only character of string.

`length(col)`: Returns the length of string in characters.

`upper(col)`: Converts a string expression to upper case.

`lower(col)`: Converts a string expression to upper case.

`ltrim(col)`: Removes leading whitespace from string.

`reverse(col)`: Returns string with the characters in reverse order.

`rtrim(col)`: Removes trailing whitespace from string.

`trim(col)`: Removes leading and trailing whitespace from string.

`word_stem(col)`: Returns the stem of word in the English language.

### Unicode Functions

`normalize(col)`: Transforms string with NFC normalization form.

`to_utf8(col)`: Encodes string into a UTF-8 varbinary representation.

`from_utf8(col)`: Decodes a UTF-8 encoded string from binary. Invalid UTF-8 sequences are replaced with the Unicode replacement character U+FFFD.

### Date and Time Functions

`date(col)`: This is an alias for CAST(x AS date).

`last_day_of_month(col)`: Returns the last day of the month.

`from_iso8601_timestamp(col)`: Parses the ISO 8601 formatted string into a timestamp with time zone.

### Date and Time Convenience Extraction Functions

`day(col)`: Returns the day of the month from x.

`day_of_month(col)`: This is an alias for day().

`day_of_week(col)`: Returns the ISO day of the week from x. The value ranges from 1 (Monday) to 7 (Sunday).

`day_of_year(col)`: Returns the day of the year from x. The value ranges from 1 to 366.

`dow(col)`: This is an alias for day_of_week().

`doy(col)`: This is an alias for day_of_year().

`hour(col)`: Returns the hour of the day from x. The value ranges from 0 to 23.

`millisecond(col)`: Returns the millisecond of the second from x.

`minute(col)`: Returns the minute of the hour from x.

`month(col)`: Returns the month of the year from x.

`quarter(col)`: Returns the quarter of the year from x. The value ranges from 1 to 4.

`second(col)`: Returns the second of the minute from x.

`timezone_hour(col)`: Returns the hour of the time zone offset from timestamp.

`timezone_minute(col)`: Returns the minute of the time zone offset from timestamp.

`week(col)`: Returns the ISO week of the year from x. The value ranges from 1 to 53.

`week_of_year(col)`: This is an alias for week().

`year(col)`: Returns the year from x.

`year_of_week(col)`: Returns the year of the ISO week from x.

`yow(col)`: This is an alias for year_of_week().

### URL Functions

_[protocol:]//host[:port]][path][?query][#fragment]_

`url_extract_fragment(col)`: Returns the fragment identifier from url.

`url_extract_host(col)`: Returns the host from url.

`url_extract_path(col)`: Returns the path from url.

`url_extract_port(col)`: Returns the port number from url.

`url_extract_protocol(col)`: Returns the protocol from url.

`url_extract_query(col)`: Returns the query string from url.

`url_encode(col)`:

`url_decode(col)`: Unescapes the URL encoded value. This function is the inverse of url_encode().

### UUID Functions

`uuid(col)`: Returns a pseudo randomly generated UUID (type 4).

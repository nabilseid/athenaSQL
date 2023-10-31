# athenaSQL

<!-- start intro -->

athenaSQL is Athena SQL query builder, inspired by sparkSQL. It borrow some sparkSQL's concept [sparkSQL](https://spark.apache.org/docs/preview/api/python/_modules/index.html).

It was initially designed to eliminate the need for hard-coding SQL queries as strings within Python scripts and as an alternative to any bespoke SQL query templating. However, it offers the flexibility to be used in various ways as needed.

<!-- end intro -->

## Quickstart

<!-- start quickstart -->

### Installing athenaSQL

```bash
$ pip install athenaSQL
```

### Usage

<!-- start usage -->

Using athenaSQL is stright forward. First we create a table abstraction class then building a query is just calling chain methods on top of it.

```python
from athenaSQL import Athena

# creating athena table instance from database
table = Athena('database_name').table('table_name')

# creating athena table instance from database
query = table.select()

query.show_query()
```

```
SELECT
    *
FROM "database_name"."table_name"
```

<!-- end usage -->

<!-- end quickstart -->

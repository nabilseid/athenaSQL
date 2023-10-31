Columns
=======

Column type is used to represent a single column. It is constracted just
by passing the column name as an argument. Column type are needed to
support python built-in operation, to identify what type of column has
passed and to constract column level queries like window functions and
aliasing.

There are different kinds of columns. We are only gonna use two of them.
There rest are constracted behind the scene. A plane column and new
column.

Plane column is a column type with only a column name, we need this when
we want to do comparison, arithmetic and logical operations on a column
otherwise we can use the column name instead. We can use either
``col()`` or ``column()`` functions to create plane column.

.. code:: python

   	import athenaSQL.functions as F

   	salary_table.select('city',                    # using column name
						F.col('country'),          # using col()
                       	F.max('salary_avg'),       # passing column name to function
                       	F.max(F.col('salary_avg')) # passing column to function
                       	).show_query()

.. code:: sql
   
   	SELECT
       	city,
       	country,
       	MAX(salary_avg),
       	MAX(salary_avg)
   	FROM "employee_db"."salary"

Column with Data Type
---------------------

New column is used when creating an EXTERNAL table. It requires both
column name and data type to create new column. We can use ``nCol()``
function to create new column.

Data types are imported from ``athenaSQL.column_type``. Supported data
types are

-  ``boolean``
-  ``tinyint``
-  ``smallint``
-  ``int``
-  ``bigint``
-  ``double``
-  ``float``
-  ``string``
-  ``binary``
-  ``date``
-  ``timestamp``
-  ``char [ (length) ]``
-  ``varchar [ (length) ]``
-  ``decimal [ (precision, scale) ]``

`More
reference <https://docs.aws.amazon.com/athena/latest/ug/create-table.html>`__

.. code:: python

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

.. code:: sql

   	CREATE EXTERNAL TABLE IF NOT EXISTS
	"product_db"."product" (
		`product_id` int,
      	`product_name` varchar(50),
      	`product_detail` string
   	)

Column Operations
-----------------

Comparison, arithmetic and logical operations can be done using python’s
built-in operators on a column. If column is string it should be wrapped 
with a column type.

Supported operations

- Comparison Operations: ``<``, ``<=``, ``=``, ``>``, ``>=``, ``!=``
- Arithmetic Operations: ``+``, ``-``, ``*``, ``/``, ``%``, ``pow()``, ``abs()``
- Logical Operations: ``&``, ``|``, ``~``

All logical operations should be enclosed by parenthesis. Parenthesis
has a higher precedence than all supported operations.

Reverese operation is supported on all comparison, all arithmetic except
``abs()`` and all logical except ``~``.

Comparison Operations
~~~~~~~~~~~~~~~~~~~~~

.. code:: python

   	print(F.col('age') >= 10)
   	print(F.col('lang') !=  'python')
   	print(F.max('age') < 50)
   	print(50 < F.max('age')) # reverse '<'

.. code:: sql

   	age >= 10
   	lang <> 'python'
   	MAX(age) < 50
   	MAX(age) > 50 -- reverse '<'

Arithmetic Operations
~~~~~~~~~~~~~~~~~~~~~

.. code:: python

   	print(F.col('revenue') - F.col('cost'))
   	print((F.col('revenue') /  5).alias('rev_5'))
   	print(pow(F.max('age'), 4))
   	print(abs(F.col('cost') - 5000))
   	print((1 - F.col('profite_fraction')).alias('cost_fraction')) # reverse '-'

.. code:: sql

   	revenue - cost
   	revenue / 5 AS rev_5
   	POWER(MAX(age), 4)
   	ABS(cost - 5000)
   	1 - profite_fraction AS cost_fraction -- reverse '-'

Logical Operations
~~~~~~~~~~~~~~~~~~

.. code:: python

   	print(~(F.col('lang') ==  'python'))
   	print((F.col('age') > 10) & (F.col('age') < 20))
   	print(((F.col('age') > 10) & (F.col('age') < 20)) | ~(F.col('is_infant') == True))

.. code:: sql

   	NOT(lang = 'python')
   	(age > 10 AND age < 20)
   	((age > 10 AND age < 20) OR NOT(is_infant = True))


In addition to the column types mentioned there are other types which
are used to identify different columns.

-  ``CaseColumn``: return type from a case function
-  ``AliasColumn``: an aliased column
-  ``WindowColumn``: a column with window function
-  ``AggregateColumn``: a column wrapped with an aggregation function
-  ``FunctionalColumn``: a column wrapped with a function
-  ``ConditionalColumn``: column with a logical operation
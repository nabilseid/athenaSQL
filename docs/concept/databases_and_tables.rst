Databases & Tables
==================

athenaSQL uses ``Athena`` class to abstracted athena databases and
``AthenaTable`` for athena tables. CTE temporary tables are abstracted
by ``TempTable``.

``AthenaTable`` cannot be constracted without a database. ``TempTable``
on the other hand doesn’t have database.

	All queries are performed on ``AthenaTable`` instance.

	Only ``SELECT`` query is availabile for CTE table. To use CTE with
	other queries pass it as a select query argument.

Using AthenaTable
~~~~~~~~~~~~~~~~~

.. code:: python

	from athenaSQL import Athena, AthenaTable, TempTable

   	# creating athena table instance from database
   	table = Athena('db_name').table('table_name')

   	# creating athena table instance directly
   	table = AthenaTable('db_name', 'table_name')

   	# creating temp table instance
   	temp_table = TempTable('temp_tbl_name')
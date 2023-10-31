Queries
=======

All queries need either ``AthenaTable`` or ``TempTable`` to execute. Query 
instance has ``show_query()`` that let's you preview the sql query constracted 
and ``exec()`` to execute constracted query on Athena, ``exec()`` returns a dataframe.

Supported queries
~~~~~~~~~~~~~~~~~
* :doc:`select`
* :doc:`insert_into`
* :doc:`create`
* :doc:`create_as`
* :doc:`cte`
* :doc:`unload`
* :doc:`window`


.. toctree::
   :maxdepth: 1
   :hidden:

   select
   insert_into
   create
   create_as
   cte
   unload
   window
Queries
=======

Public Classes
--------------

.. currentmodule:: athenaSQL.queries

.. autosummary::
   :toctree: api/

   CreateQuery
   CreateAsQuery
   InsertQuery
   SelectQuery
   CTEQuery
   UnloadQuery
   WindowQuery

CREATE Query APIs
-----------------

.. currentmodule:: athenaSQL.queries

.. autosummary::
   :toctree: api/

   CreateQuery.ifNotExists
   CreateQuery.columns
   CreateQuery.partitionBy
   CreateQuery.clusterBy
   CreateQuery.row_format
   CreateQuery.stored_as
   CreateQuery.stored_as_io
   CreateQuery.serde_properties
   CreateQuery.location
   CreateQuery.tbl_properties

CREATE AS Query APIs
--------------------

.. currentmodule:: athenaSQL.queries

.. autosummary::
   :toctree: api/

   CreateAsQuery.withData
   CreateAsQuery.createAs
   CreateAsQuery.withTblProps

INSERT Query APIs
-----------------

.. currentmodule:: athenaSQL.queries

.. autosummary::
   :toctree: api/

   InsertQuery.insert
   InsertQuery.column_order

SELECT Query APIs
-----------------

.. currentmodule:: athenaSQL.queries

.. autosummary::
   :toctree: api/

   SelectQuery.select
   SelectQuery.filter
   SelectQuery.groupBy
   SelectQuery.filterGroup
   SelectQuery.agg
   SelectQuery.limit
   SelectQuery.orderBy

CTE Query APIs
--------------

.. currentmodule:: athenaSQL.queries

.. autosummary::
   :toctree: api/

   cte.withTable
   CTEQuery.withTable
   CTEQuery.table

UNLOAD Query APIs
-----------------

.. currentmodule:: athenaSQL.queries

.. autosummary::
   :toctree: api/

   unload.unload
   UnloadQuery.unload
   UnloadQuery.location
   UnloadQuery.withTblProps

Window Query APIs
-----------------

.. currentmodule:: athenaSQL.queries

.. autosummary::
   :toctree: api/

   window.Window
   WindowQuery.partitionBy
   WindowQuery.orderBy
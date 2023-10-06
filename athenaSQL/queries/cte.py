import copy

from athenaSQL.queries.select import SelectQuery
from athenaSQL.queries.query_abc import QueryABC, \
    _exit_on_uncalled_preceding_method


class CTEQuery(QueryABC):
    """
    """

    def __init__(self):
        self._temp_tables = {}
        self._select_query = None

    def _to_sql(self):
        pass

    def withTable(self, table_name, select_query):
        """
        """

        # TODO table_name validation

        if not isinstance(select_query, SelectQuery):
            raise TypeError(f'{type(select_query).__name__} is not a type '
                            f'of SelectQuery')

        clone_obj = copy.deepcopy(self)
        self._temp_tables[table_name] = select_query
        clone_obj._temp_tables = self._temp_tables

        return clone_obj

    def table(self, table_name):
        """
        """
        # raise error there is not table to select from
        _exit_on_uncalled_preceding_method(self._temp_tables,
                                           'table',
                                           'withTable')
        # TODO table_name validation

        # raise error if selected table is not in temp tables
        if table_name not in self._temp_tables.keys():
            raise ValueError(f'temporary table `{table_name}` does not exist')

        _query = SelectQuery(table_name)
        _query._cte_tables = self._temp_tables

        return _query


def withTable(table_name, select_query):
    """
    constract a CTE query
    """
    return CTEQuery().withTable(table_name, select_query)

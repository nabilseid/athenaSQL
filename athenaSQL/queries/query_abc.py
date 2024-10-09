from abc import ABC, abstractmethod

from athenaSQL.errors import PartialQueryError
from athenaSQL.column import Column, AliasColumn
from athenaSQL.utils import normalize_sql


def _exit_on_uncalled_preceding_method(
    method_called, method_name, preceding_method_name
):
    """
    throw exception if preceding method is not called.

    params
    ------
    method_called: any
        a variable if not satisfy negation indicates preceding method has been
        called

    method_name: string
        function name of calling method

    preceding_method_name: string
        function name of preceding method
    """

    if not method_called:
        raise SyntaxError(
            f".{method_name}() cannot be called before " f".{preceding_method_name}()"
        )


def _exit_on_partial_query(query_attribute, method_name, query_name):
    """
    throw PartialQueryError if query_attribute satisfy nagation
    """

    if not query_attribute:
        raise PartialQueryError(f"{method_name}() is not called on {query_name}")


def _exit_on_override(previous_value, sql_clause, method_name):
    # exit if the previous value is not empty
    # this prevent overriding & multiple call of certain methods on chain
    if previous_value:
        raise AttributeError(
            f"You can't reassign {sql_clause} in a query, "
            f"Use .{method_name} only once in your query"
        )


def _check_and_extract_list_or_valid_typed_arguments(
    args, method_name, valid_types=(str, Column, AliasColumn)
):

    if not isinstance(valid_types, tuple):
        valid_types = (valid_types,)

    try:
        # argument is a single type, check for valid type and return
        if isinstance(args, valid_types):
            return args

        # argument is a list
        # check and return if all parameters are with in valid types
        if all(isinstance(arg, valid_types) for arg in args):
            return list(args)

        # check if there is a single parameter and of a type list
        if len(args) == 1 and isinstance(args[0], list):
            # check all items in the list valid types
            if all(isinstance(arg, valid_types) for arg in args[0]):
                return args[0]  # return the first parameter of a type list
    except:
        # escape if there are any errors, error message is raised below
        pass

    # if condition above doesn't fulfilled raise an error
    raise TypeError(
        f".{method_name}() can only accept a list or "
        f"[{', '.join(map(lambda t: t.__name__, valid_types))}]"
    )


class QueryABC(ABC):
    """
    abstract class for all query types.
    all query class should accept database name, table names and _to_sql method
    to render sql
    """

    def __init__(self, database, table):
        self.database = database
        self.table = table
        self.query_engine = None

        ABC.__init__(self)

    @abstractmethod
    def _to_sql(self, **kwargs):
        pass

    def show_query(self):
        """preivew sql query"""
        print(self._to_sql())

    def normalize_query(self):
        """Normalize the SQL query by removing extra whitespace, tabs, and newlines."""
        return normalize_sql(self._to_sql())

    # TODO pkg athena and add as dependency
    # def exec(self, return_type='dataframe', dry_run=False):
    #     """
    #     run athena query, if dry_run is true return query string
    #     """
    #     query = self._to_sql()

    #     if dry_run:
    #         return self._to_sql()

    #     self.query_engine =  RunAthenaQuery(query=query, return_type=return_type)

    #     return self.query_engine.query_results()

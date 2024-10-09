from athenaSQL.queries import SelectQuery, InsertQuery, CreateQuery, CreateAsQuery


class AthenaTable:
    """
    abstract class for athena table
    """

    def __init__(self, *, database: str, table: str):
        self.database: str = database
        self.table: str = table

    def select(self, *cols):
        return SelectQuery(self.table, database=self.database).select(*cols)

    def insert(self, select_query):
        return InsertQuery(self.database, self.table).insert(select_query)

    def create(self):
        return CreateQuery(self.database, self.table)

    def createAs(self, select_query):
        return CreateAsQuery(self.database, self.table).createAs(select_query)


class TempTable:
    """
    abstract class for temporary table.
    used to represent cte tables
    """

    def __init__(self, table: str):
        self.table: str = table

    def select(self, *cols):
        return SelectQuery(self.table).select(*cols)

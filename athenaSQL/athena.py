from athenaSQL.athena_table import AthenaTable

class Athena:
    """
    An abstraction layer for athena database & table.
    It has a static method that accepts a query string to run agains the whole
    database.

    Parameters
    ----------
    database: str 
        database name to select desired table from 

    Example
    -------
    >>> from adflow.sql.athena import Athena
    >>> cpe_table = Athena('adhouse').table('cpe') # abstraction table class for cpe
    >>> cpe_df = Athena.sql('SELECT * FROM adhouse.cpe') # cpe data in pandas df
    """
    def __init__(self, database):
        self.database = database

    def table(self, table):
        """
        """
        return AthenaTable(database=self.database, table=table)

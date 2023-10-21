from importlib.metadata import version

from .athena import Athena
from .athena_table import AthenaTable, TempTable
from .column import Column
from .errors import PartialQueryError

__version__ = version(__name__)

from enum import Enum

class DataType(Enum):
    """Supported data types enum class.
    """
    BOOLEAN = 'boolean'
    TINYINT = 'tinyint'
    SMALLINT = 'smallint'
    INT = 'int'
    INTEGER = 'integer'
    BIGINT = 'bigint'
    DOUBLE = 'double'
    FLOAT = 'float'
    CHAR = 'char'
    VARCHAR = 'varchar'
    STRING = 'string'
    BINARY = 'binary'
    DATE = 'date'
    TIMESTAMP = 'timestamp'
    DECIMAL = 'decimal'
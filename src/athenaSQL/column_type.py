class ColumnType:
    """
    column type class, used to identify column types 
    """
    def __init__(self, col_type):
        self.col_type = col_type

        self._syntax = col_type 

    def __str__(self):
        return self._syntax

class UnaryColumnType(ColumnType):
    """
    column type that takes single operand like varchar(10).
    instantiated with column type as input.
    instance is callable with operand as input.
    """
    def __init__(self, col_type):
        super().__init__(col_type)

    def __call__(self, operand):
        self._syntax = f'{self.col_type}({operand})'
        return self

class BinaryColumnType(ColumnType):
    """
    column type that takes two operands like decimal(10, 25)
    instantiated with column type as input
    instance is ballable with operands as inputs 
    """
    def __init__(self, col_type):
        super().__init__(col_type)

    def __call__(self, operand1, operand2):
        self._syntax = f'{self.col_type}({operand1}, {operand2})'
        return self

class _COLUMNTYPE:
    boolean = ColumnType('boolean')
    tinyint = ColumnType('tinyint')
    smallint = ColumnType('smallint')
    int = ColumnType('int')
    bigint = ColumnType('bigint')
    double = ColumnType('double')
    float = ColumnType('float')
    string = ColumnType('string')
    binary = ColumnType('binary')
    date = ColumnType('date')
    timestamp = ColumnType('timestamp')
    char = UnaryColumnType('char')
    varchar = UnaryColumnType('varchar')
    decimal = BinaryColumnType('decimal')
    #TODO array, map, struct

COLUMNTYPE = _COLUMNTYPE()

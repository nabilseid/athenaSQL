
from typing import Union

class ColumnType:
    """
    column type class, used to identify column types 
    """
    def __init__(self, col_type: str):
        self.col_type: str = col_type

        self._syntax: str = col_type 

    def __str__(self) -> str:
        return self._syntax

class UnaryColumnType(ColumnType):
    """
    column type that takes single operand like varchar(10).
    instantiated with column type as input.
    instance is callable with operand as input.
    """
    def __init__(self, col_type: str):
        super().__init__(col_type)

    def __call__(self, operand: Union[str, int]):
        self._syntax: str = f'{self.col_type}({operand})'
        return self

class BinaryColumnType(ColumnType):
    """
    column type that takes two operands like decimal(10, 25)
    instantiated with column type as input
    instance is ballable with operands as inputs 
    """
    def __init__(self, col_type: str):
        super().__init__(col_type)

    def __call__(self, operand1: int, operand2: int):
        self._syntax: str = f'{self.col_type}({operand1}, {operand2})'
        return self

class _COLUMNTYPE:
    boolean: ColumnType = ColumnType('boolean')
    tinyint: ColumnType = ColumnType('tinyint')
    smallint: ColumnType = ColumnType('smallint')
    int: ColumnType = ColumnType('int')
    bigint: ColumnType = ColumnType('bigint')
    double: ColumnType = ColumnType('double')
    float: ColumnType = ColumnType('float')
    string: ColumnType = ColumnType('string')
    binary: ColumnType = ColumnType('binary')
    date: ColumnType = ColumnType('date')
    timestamp: ColumnType = ColumnType('timestamp')
    char: UnaryColumnType = UnaryColumnType('char')
    varchar: UnaryColumnType = UnaryColumnType('varchar')
    decimal: BinaryColumnType = BinaryColumnType('decimal')
    #TODO array, map, struct

COLUMNTYPE = _COLUMNTYPE()

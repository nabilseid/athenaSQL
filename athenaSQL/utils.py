import re
from typing import Union, Optional

from athenaSQL.column import Column


def stringify(
    colOrPrimitive: Union[Column, int, str, float, bool],
    singleQuote: Optional[bool] = True,
):
    """Return a string representation of `colOrPrimitive`. If `colOrPrimitive`
    is a string it will be enclosed in a pair of single quotes.

    Args:
        colOrPrimitive (Union[Column, int, str, float, bool]): a column or
        primitive to stringify.
        singleQuote (Optional[bool]): whether to use single or double quotes
        for string values. Default is True.

    Raises:
        TypeError: raised if `colOrPrimitive` is not of supported types.

    Returns:
        str: return string representation of `colOrPrimitive`.
    """
    if not isinstance(colOrPrimitive, (Column, int, str, float, bool)):
        raise TypeError(
            f"{type(colOrPrimitive).__name__} is not a type of "
            "(Column, int, str, float, bool)"
        )

    return (
        f"'{colOrPrimitive}'"
        if isinstance(colOrPrimitive, str)
        else str(colOrPrimitive)
    )


def normalize_sql(query: str) -> str:
    # Remove extra whitespace, tabs, and newlines
    return re.sub(r"\s+", " ", query).strip()

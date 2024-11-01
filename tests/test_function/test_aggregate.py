import athenaSQL.functions as F
from athenaSQL.functions.aggregate import _unary_functions
from athenaSQL.utils import normalize_sql


def test_unary_functions():
    # Loop over each unary function definition
    for func_name, doc in _unary_functions.items():
        # Check if the function was created and is accessible
        func = getattr(F, func_name, None)
        assert func is not None, f"{func_name} function is not created."

        # Check if the function is callable with a sample argument
        col = F.col("sample_column")
        expression = str(func(col))

        # Check if the resulting SQL representation is as expected
        expected_expression = normalize_sql(f"{func_name.upper()}(sample_column)")
        assert (
            normalize_sql(expression) == expected_expression
        ), f"SQL representation for {func_name} is incorrect."

        # Check if the docstring is correctly assigned
        assert func.__doc__ == doc, f"Docstring for {func_name} is incorrect."

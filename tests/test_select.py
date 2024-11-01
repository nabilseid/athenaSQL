import pytest
from athenaSQL import Athena
from athenaSQL.functions import col
from athenaSQL.orders import DESC, ASC
from athenaSQL.utils import normalize_sql


@pytest.fixture
def athena():
    return Athena("database_name").table("table_name")


def test_select_all_from_table(athena):
    expected_query = 'SELECT * FROM "database_name"."table_name"'

    query = athena.select().normalize_query()

    assert query == normalize_sql(expected_query)


def test_select_columns(athena):
    expected_query = normalize_sql(
        'SELECT column1, column2 FROM "database_name"."table_name"'
    )

    query = athena.select("column1", col("column2")).normalize_query()

    assert query == expected_query


def test_select_alias_column(athena):
    expected_query = normalize_sql(
        'SELECT column1 AS alias_col1, column2 FROM "database_name"."table_name"'
    )

    query = athena.select(
        col("column1").alias("alias_col1"), col("column2")
    ).normalize_query()

    assert query == expected_query


def test_select_specific_columns(athena):
    expected_query = """
    SELECT column1, column2 
    FROM "database_name"."table_name"
    WHERE column1 = 'value'
    GROUP BY column1, column2
    HAVING column2 > 10
    ORDER BY column1 DESC, column2 ASC
    LIMIT 10
    """

    query = (
        athena.select("column1", "column2")
        .filter(col("column1") == "value")
        .groupBy("column1", "column2")
        .filterGroup(col("column2") > 10)
        .orderBy(DESC("column1"), ASC("column2"))
        .limit(10)
        .normalize_query()
    )

    assert query == normalize_sql(expected_query)

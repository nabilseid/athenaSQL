import pytest
from athenaSQL import Athena


@pytest.fixture
def athena():
    return Athena("database_name").table("table_name")


def test_select_all_from_table(athena):
    expected_query = 'SELECT * FROM "database_name"."table_name"'

    query = athena.select().normalize_query()

    assert query == expected_query

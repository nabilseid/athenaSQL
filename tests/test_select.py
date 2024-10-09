from athenaSQL import Athena


def test_select_all_from_table():
    expected_query = 'SELECT * FROM "database_name"."table_name"'

    query = Athena("database_name").table("table_name").select().normalize_query()

    assert query == expected_query

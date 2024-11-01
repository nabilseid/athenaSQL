# conftest.py
import pytest
from athenaSQL import Athena


@pytest.fixture(scope="session")
def Table():
    return Athena("database_name").table("table_name")

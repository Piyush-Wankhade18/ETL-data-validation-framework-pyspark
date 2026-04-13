import pytest
from pyspark.sql import SparkSession

from src.validation.row_count import validate_row_count
from src.validation.null_check import validate_nulls
from src.validation.duplicate_check import validate_duplicates
from src.validation.data_compare import validate_data
from src.validation.aggregate_check import validate_aggregate


# ---------------------------------
# Spark Session Fixture
# ---------------------------------
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("ETLValidationTest") \
        .getOrCreate()


# ---------------------------------
# Sample Data Fixtures
# ---------------------------------
@pytest.fixture
def valid_data(spark):
    data = [
        (1, "A", 100),
        (2, "B", 200),
        (3, "C", 300)
    ]
    columns = ["customer_id", "name", "amount"]

    df_source = spark.createDataFrame(data, columns)
    df_target = spark.createDataFrame(data, columns)

    return df_source, df_target


@pytest.fixture
def duplicate_data(spark):
    data = [
        (1, "A", 100),
        (1, "A", 100)
    ]
    columns = ["customer_id", "name", "amount"]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def null_data(spark):
    data = [
        (1, None, 100)
    ]
    columns = ["customer_id", "name", "amount"]

    return spark.createDataFrame(data, columns)


# ---------------------------------
# TEST CASES
# ---------------------------------

def test_row_count(valid_data):
    df_source, df_target = valid_data
    assert validate_row_count(df_source, df_target, logger=None) is True


def test_data_compare(valid_data):
    df_source, df_target = valid_data
    assert validate_data(df_source, df_target, logger=None) is True


def test_null_check(null_data):
    assert validate_nulls(null_data, "name", logger=None) is False


def test_duplicate_check(duplicate_data):
    assert validate_duplicates(duplicate_data, "customer_id", logger=None) is False


def test_aggregate(valid_data):
    df_source, df_target = valid_data
    assert validate_aggregate(df_source, df_target, "amount", logger=None) is True

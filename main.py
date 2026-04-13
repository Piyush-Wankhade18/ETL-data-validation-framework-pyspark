
from src.utils.spark_session import get_spark
from src.utils.logger import get_logger
from src.utils.config_reader import load_config

from src.validation.row_count import validate_row_count
from src.validation.data_compare import validate_data
from src.validation.null_check import validate_nulls
from src.validation.duplicate_check import validate_duplicates
from src.validation.aggregate_check import validate_aggregate



spark = get_spark()
logger = get_logger()

config = load_config()


spark = get_spark()
logger = get_logger()

df_source = spark.read.table(config["source"]["name"])
df_target = spark.read.table(config["target"]["name"])

validate_row_count(df_source, df_target, logger)
validate_data(df_source, df_target, logger)
validate_nulls(df_target, "customer_id", logger)
validate_duplicates(df_target, "customer_id", logger)
validate_aggregate(df_source, df_target, "amount", logger)

print("Validation Completed")

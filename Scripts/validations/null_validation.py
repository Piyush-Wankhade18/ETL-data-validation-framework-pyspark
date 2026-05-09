from pyspark.sql.functions import col

def validate_nulls(df, column, logger):
    null_count = df.filter(col(column).isNull()).count()

    if null_count == 0:
        logger.info(f"No nulls in {column}")
        return True
    else:
        logger.error(f"Nulls found in {column}")
        return False

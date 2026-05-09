from pyspark.sql.functions import sum

def validate_aggregate(df_source, df_target, column, logger):
    source_sum = df_source.select(sum(column)).collect()[0][0]
    target_sum = df_target.select(sum(column)).collect()[0][0]

    if source_sum == target_sum:
        logger.info("Aggregate matched")
        return True
    else:
        logger.error("Aggregate mismatch")
        return False

def validate_duplicates(df, column, logger):
    dup_count = df.groupBy(column).count().filter("count > 1").count()

    if dup_count == 0:
        logger.info("No duplicates found")
        return True
    else:
        logger.error("Duplicates found")
        return False

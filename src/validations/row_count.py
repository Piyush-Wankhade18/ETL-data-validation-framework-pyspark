def validate_row_count(df_source, df_target, logger):
    source_count = df_source.count()
    target_count = df_target.count()

    if source_count == target_count:
        logger.info("Row count matched")
        return True
    else:
        logger.error("Row count mismatch")
        return False

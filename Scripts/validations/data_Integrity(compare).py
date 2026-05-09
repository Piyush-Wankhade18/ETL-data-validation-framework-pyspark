def validate_data(df_source, df_target, logger):
    diff = df_source.exceptAll(df_target)

    if diff.count() == 0:
        logger.info("Data matched")
        return True
    else:
        logger.error("Data mismatch found")
        return False

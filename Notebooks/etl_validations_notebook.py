--It’s used in Databricks for interactive validation and debugging ETL pipelines.--
#1: Read Raw Data (Bronze Layer)

df_raw = spark.read.format("delta").load("/mnt/bronze/customer_data")

display(df_raw)

#2: Data Cleaning (Null Handling)
from pyspark.sql.functions import col

df_clean = df_raw.dropDuplicates()

df_clean = df_clean.filter(col("customer_id").isNotNull())

display(df_clean)

#3: Data Standardization
from pyspark.sql.functions import upper, trim

df_std = df_clean.withColumn("name", upper(trim(col("name"))))

#4: Derived Columns
from pyspark.sql.functions import when

df_enriched = df_std.withColumn(
    "customer_type",
    when(col("amount") > 1000, "HIGH_VALUE").otherwise("NORMAL")
)

#5: Incremental Load (Watermark Logic)
last_watermark = "2026-04-01 10:00:00"

df_incremental = df_enriched.filter(
    col("updated_date") > last_watermark
)
________________________________________
6: Join with Lookup Table
df_country = spark.read.table("lookup_db.country_master")

df_joined = df_incremental.join(
    df_country,
    df_incremental.country_code == df_country.country_code,
    "left"
)

#7: Aggregation (Business Metrics)
from pyspark.sql.functions import sum, count

df_agg = df_joined.groupBy("country_name") \
    .agg(
        count("customer_id").alias("total_customers"),
        sum("amount").alias("total_revenue")
    )

display(df_agg)

#8: Window Function (Latest Record)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("customer_id").orderBy(col("updated_date").desc())

df_latest = df_joined.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

display(df_latest)

9: Write to Silver Layer
df_latest.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/silver/customer_data")

#10: Merge into Gold Table (UPSERT)
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/mnt/gold/customer_gold")

target.alias("t").merge(
    df_latest.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
________________________________________
Transformation	Real Use
Data cleaning	Remove bad data
Standardization	Format consistency
Derived columns	Business logic
Watermark	Incremental load
Join	Enrichment
Aggregation	KPI generation
Window function	Latest record
Merge	Upsert (SCD-like)
________________________________________
“I have worked on data cleaning, standardization, derived columns, incremental load using watermark logic, joins with lookup tables, 
aggregations for business KPIs, and window functions for latest record selection. I have also implemented merge operations using Delta Lake for upsert scenarios.”

                        


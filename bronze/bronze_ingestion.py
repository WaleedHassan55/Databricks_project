# Bronze Layer - Data Ingestion

from pyspark.sql.functions import current_timestamp

# Step 1: Read raw data
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/data-lake/raw/sales/")

# Step 2: Add ingestion timestamp
df_bronze = df_raw.withColumn("ingestion_time", current_timestamp())

# Step 3: Write to Bronze Delta table
df_bronze.write.format("delta") \
    .mode("append") \
    .saveAsTable("de_project.bronze_layer.sales_data")

# Step 4: Show data (for testing)
df_bronze.show()

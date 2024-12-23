from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date
from delta.tables import DeltaTable

# Initialize Spark Session
spark = SparkSession.builder.appName("HarmonizeStores").getOrCreate()

# Paths for raw and harmonized tables
RAW_TABLE = "processing_catalog.schema_raw_dimension.t_stores"
HARMONIZED_TABLE = "processing_catalog.schema_harmonized_dimension.t_stores"

# Function to get the max batch timestamp from the harmonized layer
def get_max_batch_timestamp(harmonized_table):
    if DeltaTable.isDeltaTable(spark, harmonized_table):
        max_batch_ts = spark.sql(f"SELECT MAX(batch_r_timestamp) as max_batch FROM {harmonized_table}").collect()[0]["max_batch"]
        return max_batch_ts
    else:
        return None  # No data yet in harmonized table

# Function to get the latest unprocessed batch from the raw table
def get_latest_batch(raw_table, max_batch_timestamp):
    condition = f"r_insert_timestamp > '{max_batch_timestamp}'" if max_batch_timestamp else "1=1"
    latest_batch_df = spark.sql(f"SELECT * FROM {raw_table} WHERE {condition}")
    return latest_batch_df

# Function to type cast the raw data to harmonized schema
def type_cast_data(raw_df):
    return raw_df.select(
        col("store_id").cast("string"),
        col("store_name").cast("string"),
        col("location").cast("string"),
        col("manager_id").cast("string"),
        to_date(col("opening_date"), "dd-MM-yyyy").alias("opening_date"),
        col("r_insert_timestamp").alias("batch_r_timestamp"),  # Map to harmonized audit column
        current_timestamp().alias("insert_timestamp"),        # Insert timestamp
        current_timestamp().alias("update_timestamp"),        # Update timestamp
        lit("batch_job").alias("job_id")                      # Example job_id
    )

# Function to merge data into the harmonized table
def merge_into_harmonized_table(harmonized_table, latest_batch_df):
    # PYTHONIC way using DELTA API
    # harmonized_delta_table = DeltaTable.forName(spark, harmonized_table)
    
    # # Merge SQL Logic
    # harmonized_delta_table.alias("target").merge(
    #     latest_batch_df.alias("source"),
    #     "target.store_id = source.store_id"
    # ).whenMatchedUpdate(set={
    #     "store_name": "source.store_name",
    #     "location": "source.location",
    #     "manager_id": "source.manager_id",
    #     "opening_date": "source.opening_date",
    #     "batch_r_timestamp": "source.batch_r_timestamp",
    #     "h_update_timestamp": "current_timestamp()",
    #     "job_id": "source.job_id"
    # }).whenNotMatchedInsert(values={
    #     "store_id": "source.store_id",
    #     "store_name": "source.store_name",
    #     "location": "source.location",
    #     "manager_id": "source.manager_id",
    #     "opening_date": "source.opening_date",
    #     "batch_r_timestamp": "source.batch_r_timestamp",
    #     "h_insert_timestamp": "source.insert_timestamp",
    #     "h_update_timestamp": "source.update_timestamp",
    #     "job_id": "source.job_id"
    # }).execute()

    # SQL way using Spark SQL
    # Write the latest batch to a temporary view
    latest_batch_df.createOrReplaceTempView("latest_batch_view")
    
    # merge statement
    merge_sql = f"""
        MERGE INTO {harmonized_table} AS target
        USING latest_batch_view AS source
        ON target.store_id = source.store_id
        WHEN MATCHED THEN
            UPDATE SET
                target.store_name = source.store_name,
                target.location = source.location,
                target.manager_id = source.manager_id,
                target.opening_date = source.opening_date,
                target.batch_r_timestamp = source.batch_r_timestamp,
                target.h_update_timestamp = current_timestamp(),
                target.job_id = source.job_id
        WHEN NOT MATCHED THEN
            INSERT (
                store_id, store_name, location, manager_id, opening_date,
                batch_r_timestamp, h_insert_timestamp, h_update_timestamp, job_id
            ) VALUES (
                source.store_id, source.store_name, source.location, source.manager_id, source.opening_date,
                source.batch_r_timestamp, source.insert_timestamp, source.update_timestamp, source.job_id
            )
    """

    # Execute the merge statement
    spark.sql(merge_sql)

# Main ETL Function
def process_harmonized_table():
    # Step 1: Get the max batch timestamp from the harmonized table
    max_batch_timestamp = get_max_batch_timestamp(HARMONIZED_TABLE)

    # Step 2: Fetch the latest batch from the raw table
    latest_batch_df = get_latest_batch(RAW_TABLE, max_batch_timestamp)

    if latest_batch_df.count() == 0:
        print("No new data to process.")
        return

    # Step 3: Typecast the raw data to harmonized schema
    harmonized_data = type_cast_data(latest_batch_df)

    # display(harmonized_data)

    # Step 4: Merge the harmonized data into the harmonized table
    merge_into_harmonized_table(HARMONIZED_TABLE, harmonized_data)

    print("Data successfully processed into the harmonized table.")

# # Execute the ETL Process
process_harmonized_table()

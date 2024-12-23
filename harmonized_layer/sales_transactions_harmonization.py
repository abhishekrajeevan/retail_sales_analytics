from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable

# Initialize Spark Session
spark = SparkSession.builder.appName("HarmonizeSalestransactions").getOrCreate()

# Paths for raw and harmonized tables
RAW_TABLE = "processing_catalog.schema_raw_facts.t_sales_transactions"
HARMONIZED_TABLE = "processing_catalog.schema_harmonized_facts.t_sales_transactions"

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
        col("invoice_id").cast("string"),
        col("transaction_date_time").cast("timestamp"),
        col("store_id").cast("string"),
        col("customer_id").cast("string"),
        col("employee_id").cast("string"),
        col("payment_type").cast("string"),
        col("total_amount").cast("decimal(10, 2)"),
        col("r_insert_timestamp").alias("batch_r_timestamp"),  # Map to harmonized audit column
        lit("batch_job").alias("job_id")                      # Example job_id
    )

# Function to merge data into the harmonized table
def merge_into_harmonized_table(harmonized_table, latest_batch_df):

    # SQL way using Spark SQL
    # Write the latest batch to a temporary view
    latest_batch_df.createOrReplaceTempView("latest_batch_view")
    
    # merge statement
    merge_sql = f"""
        MERGE INTO {harmonized_table} AS target
        USING latest_batch_view AS source
        ON target.invoice_id = source.invoice_id
        WHEN MATCHED THEN
            UPDATE SET
                target.invoice_id = source.invoice_id,
                target.transaction_date_time = source.transaction_date_time,
                target.store_id = source.store_id,
                target.customer_id = source.customer_id,
                target.employee_id = source.employee_id,
                target.payment_type = source.payment_type,
                target.total_amount = source.total_amount,
                target.batch_r_timestamp = source.batch_r_timestamp,
                target.h_update_timestamp = current_timestamp(),
                target.job_id = source.job_id
        WHEN NOT MATCHED THEN
            INSERT (
                invoice_id,
                transaction_date_time,
                store_id,
                customer_id,
                employee_id,
                payment_type,
                total_amount,
                batch_r_timestamp,
                h_insert_timestamp,
                h_update_timestamp,
                job_id
            )
            VALUES (
                source.invoice_id,
                source.transaction_date_time,
                source.store_id,
                source.customer_id,
                source.employee_id,
                source.payment_type,
                source.total_amount,
                source.batch_r_timestamp,
                current_timestamp(),
                current_timestamp(),
                source.job_id
            );

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

    # Step 4: Merge the harmonized data into the harmonized table
    merge_into_harmonized_table(HARMONIZED_TABLE, harmonized_data)

    print("Data successfully processed into the harmonized table.")

# # Execute the ETL Process
process_harmonized_table()

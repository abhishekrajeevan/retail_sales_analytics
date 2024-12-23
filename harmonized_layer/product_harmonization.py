from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable

# Initialize Spark Session
spark = SparkSession.builder.appName("HarmonizeProducts").getOrCreate()

# Paths for raw and harmonized tables
RAW_TABLE = "processing_catalog.schema_raw_dimension.t_products"
HARMONIZED_TABLE = "processing_catalog.schema_harmonized_dimension.t_products"

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

# Function to do data quality checks on the latest batch
def data_quality_checks(df, primary_key, audit_column):

    # duplicate checks
    duplicate_check = df.groupBy(col(primary_key), col(audit_column)).count().filter("count > 1")
    dq_duplicates = duplicate_check.select(col(primary_key), col(audit_column)).withColumn("dq_issue", lit("Duplicate product_id"))

    # Type validation for price
    dq_price_validation = df.filter(~col("price").cast("string").rlike("^[0-9]+(\\.[0-9]+)?$")).withColumn("dq_issue", lit("Invalid price format")) \
                    .select(col(primary_key), col(audit_column), "dq_issue")


    # Check for nulls
    check_columns = ["product_name", "brand", "category","gender", "size", "color", "price", "availability"]
    
    # Create a condition to check for null values in any column
    null_condition = None
    for column in check_columns:
        if null_condition is None:
            null_condition = col(column).isNull()
        else:
            null_condition |= col(column).isNull()

    # print(null_condition)
    
    # Filter records with any null values
    dq_null_records = df.filter(null_condition).select(
                        col(primary_key),            # Include the primary key (e.g., product_id)
                        col(audit_column),           # Include the audit timestamp (e.g., r_insert_timestamp)
                        lit("Null value in one or more columns").alias("dq_issue")  # Add a DQ issue description
                    )
    
    dq_failures = dq_duplicates.unionAll(dq_price_validation).unionAll(dq_null_records).toDF("unique_identifier", "r_batch_timestamp", "issue")

    return dq_failures

# log the DQ failures to a DQ table
def log_dq_failures(dq_failures, dq_table):
    dq_failures = dq_failures.withColumn("logged_at", current_timestamp())
    dq_failures.write.format("delta").mode("append").saveAsTable(dq_table)

# Function to type cast the raw data to harmonized schema
def type_cast_data(raw_df):
    return raw_df.select(
        col("product_id").cast("string"),
        col("product_name").cast("string"),
        col("brand").cast("string"),
        col("category").cast("string"),
        col("gender").cast("string"),
        col("size").cast("string"),
        col("color").cast("string"),
        col("price").cast("double"),
        col("availability").cast("integer"),
        col("added_date").cast("date"),
        col("is_active").cast("boolean"),
        col("r_insert_timestamp").alias("batch_r_timestamp"),  # Map to harmonized audit column
        current_timestamp().alias("insert_timestamp"),        # Insert timestamp
        current_timestamp().alias("update_timestamp"),        # Update timestamp
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
        ON target.product_id = source.product_id
        WHEN MATCHED THEN
            UPDATE SET
                target.product_id = source.product_id,
                target.product_name = source.product_name,
                target.brand = source.brand,
                target.category = source.category,
                target.gender = source.gender,
                target.size = source.size,
                target.color = source.color,
                target.price = source.price,
                target.availability = source.availability,
                target.added_date = source.added_date,
                target.is_active = source.is_active,
                target.batch_r_timestamp = source.batch_r_timestamp,
                target.h_update_timestamp = current_timestamp(),
                target.job_id = source.job_id
        WHEN NOT MATCHED THEN
            INSERT (
                product_id, product_name, brand, category, gender, size, color, price, availability, added_date, is_active,
                batch_r_timestamp, h_insert_timestamp, h_update_timestamp, job_id
            ) VALUES (
                source.product_id, source.product_name, source.brand, source.category, source.gender, source.size, source.color,
                source.price, source.availability, source.added_date, source.is_active,
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

    if latest_batch_df.isEmpty():
        print("No new data to process.")
        return
    
    # Perform data quality checks
    DQ_TABLE = "processing_catalog.schema_audit.dq_logs"
    dq_failures = data_quality_checks(latest_batch_df, "product_id", "r_insert_timestamp")

    if not dq_failures.isEmpty():
        print("Data quality checks failed. Logging issues and stopping the batch.")
        log_dq_failures(dq_failures, DQ_TABLE)
        return  # Stop processing

    # Step 3: Typecast the raw data to harmonized schema
    harmonized_data = type_cast_data(latest_batch_df)

    # Step 4: Merge the harmonized data into the harmonized table
    merge_into_harmonized_table(HARMONIZED_TABLE, harmonized_data)

    print("Data successfully processed into the harmonized table.")

# # Execute the ETL Process
process_harmonized_table()
-- SCD2 Implementation Explanation:
-- This script implements Slowly Changing Dimension Type 2 (SCD2) on a customer dimension table.
-- Key steps:
-- 1. Identify new records and changes in the source table (`new_batch`) compared to the target table.
-- 2. Combine the new records with the active, changed records from the target table (`combined_data`).
-- 3. Use a MERGE statement to:
--    a. Mark existing rows as inactive (`active_flag = false`) and set their `end_effective_date`.
--    b. Insert new or updated records with `active_flag = true` and `begin_effective_date = current_date()`.

-- Create a temporary view for the combined data (new and changed records)
create temporary view t_customers_temp as 
with new_batch as (
  -- Extract new data from the source table that arrived after the last processed batch
  select
    t_customers.customer_id,
    t_customers.first_name,
    t_customers.last_name,
    t_customers.email,
    t_customers.phone_number,
    t_customers.loyalty_points,
    t_customers.membership_status,
    t_customers.address,
    -- Generate a hash value of all significant columns to detect changes in existing records
    md5(concat(
      customer_id, 
      first_name, 
      last_name, 
      email, 
      phone_number, 
      loyalty_points, 
      membership_status, 
      address
    )) as hash_value,
    -- Record the insert timestamp of the source data
    t_customers.r_insert_timestamp
  from
    processing_catalog.schema_raw_dimension.t_customers
  where
    -- Filter for records that were inserted after the last processed batch in the target table
    r_insert_timestamp > (
      select
        max(batch_r_timestamp)
      from
        processing_catalog.schema_harmonized_dimension.t_customers
    )
),
combined_data as (
  -- Select all new records from the source table
  select
    customer_id as merge_customer_id, -- Set this for the MERGE operation
    *
  from
    new_batch
  union all
  -- Select existing active records from the target table that have changed
  select
    null as merge_customer_id, -- To ensure these do not match directly in the MERGE
    *
  from
    new_batch
  inner join
    processing_catalog.schema_harmonized_dimension.t_customers scd2
  on
    new_batch.customer_id = scd2.customer_id
    -- Match only active records
    and scd2.active_flag = true
    -- Match only records where the hash value indicates changes
    and new_batch.hash_value != scd2.s_customer_key
)
-- Materialize the combined data into a temporary view for the MERGE
select
  *
from
  combined_data;

-- Perform the SCD2 Merge operation
merge into processing_catalog.schema_harmonized_dimension.t_customers target 
using t_customers_temp source 
on source.merge_customer_id = target.customer_id
  -- Update existing active records if their attributes have changed
  when matched
  AND (
    target.active_flag = true 
    AND target.s_customer_key != source.hash_value
  )
then
update
set
  target.active_flag = false, -- Mark the current record as inactive
  target.end_effective_date = current_date() -- Set the end date for the existing record
  -- Insert new or changed records
  when not matched then
insert (
    s_customer_key,         -- Hash value of the new record
    customer_id,            -- Unique customer identifier
    first_name,             -- Customer first name
    last_name,              -- Customer last name
    email,                  -- Customer email
    phone_number,           -- Customer phone number
    loyalty_points,         -- Loyalty points
    membership_status,      -- Membership status
    address,                -- Customer address
    active_flag,            -- Mark the new record as active
    begin_effective_date,   -- Effective start date of the record
    end_effective_date,     -- Default future date (record is active)
    batch_r_timestamp,      -- Timestamp of the batch
    h_insert_timestamp,     -- Insert timestamp for this record
    h_update_timestamp,     -- Last update timestamp for this record
    job_id                  -- Identifier for the batch job
  )
values (
    source.hash_value,           -- New hash value
    source.customer_id,          -- Customer ID from the source
    source.first_name,           -- First name from the source
    source.last_name,            -- Last name from the source
    source.email,                -- Email from the source
    source.phone_number,         -- Phone number from the source
    source.loyalty_points,       -- Loyalty points from the source
    source.membership_status,    -- Membership status from the source
    source.address,              -- Address from the source
    true,                        -- Set the active flag to true
    current_date(),              -- Set the effective start date to today
    date('9999-12-31'),          -- Default future date
    source.r_insert_timestamp,   -- Batch timestamp from the source
    current_timestamp(),         -- Current timestamp for insert
    current_timestamp(),         -- Current timestamp for update
    "batch2"                     -- Batch job ID (hardcoded for now)
  );

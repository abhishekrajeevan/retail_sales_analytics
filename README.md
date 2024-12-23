# Retail Sales Analytics


# Data Architecture for Sales Analytics Project

## Project Overview
This project involves setting up a scalable and governed data pipeline for sales data processing using a modern data stack on GCP and Databricks. The primary goal is to extract, process, and analyze sales data from various sources while ensuring data governance and security.

### Key Components

1. **Data Sources**
   - PostgreSQL, SFTP, SharePoint

2. **Data Ingestion Layer**
   - Data is extracted from PostgreSQL using a Cloud Function (Python).
   - The extraction process is scheduled using Cloud Scheduler to run daily.
   - The extracted data is stored in the GCS Landing Zone with daily partitioning. Separate folders/files are created for each day.

3. **Landing Zone (GCS)**
   - Acts as a staging area for raw data.
   - Organized into partitioned folders for better management.
   - Data access is controlled via service accounts with credentials securely stored in Secret Manager.

4. **Processing on Databricks**
   - The processing architecture follows the Medallion approach:
     - **Raw Layer**: Stores unprocessed data.
     - **Harmonized Layer**: Contains cleansed and transformed data.
     - **Curated Layer**: Final dataset optimized for consumption.
     - Additional layers may be added as per requirements.
   - Data is stored as Delta Tables and managed by Unity Catalog.
   - Separate schemas for `raw`, `harmonized`, `curated`, and `audit` (for Data Quality logs) are maintained within the **processing_catalog**.

5. **Governance Layer**
   - Unity Catalog is used for:
     - Table management with proper tagging.
     - Masking sensitive columns.
     - Creating groups and managing access controls for members.

6. **Orchestration**
   - Databricks Workflows orchestrate data movement and transformations across the layers.

7. **Consumption Layer**
   - Data is accessible to Data Analysts, APIs, and visualization tools like Tableau.

## Data Governance and Security
- **Unity Catalog**: A single metastore is created and linked to the workspace for centralized governance.
- **Service Accounts**: Access to the GCS Landing Zone is restricted to authenticated service accounts.
- **Secret Manager**: Used to securely share credentials.
- **Data Masking**: Sensitive columns are masked.

## Catalog Design
1. **landing_catalog**: 
   - Manages external data in the GCS Landing Zone as an external volume.
2. **processing_catalog**:
   - Manages Delta Tables for `raw`, `harmonized`, and `curated` schemas.

## Architecture Diagram



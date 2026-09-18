-- 1. Create Unity Catalog
CREATE CATALOG IF NOT EXISTS cfb_transfer_portal_pipeline;

-- 2. Set active catalog context
USE CATALOG cfb_transfer_portal_pipeline;

-- 3. Create Schemas for workspace layers
CREATE SCHEMA IF NOT EXISTS _01_raw_bronze;
CREATE SCHEMA IF NOT EXISTS _02_staging_silver;
CREATE SCHEMA IF NOT EXISTS _03_analytics_gold;
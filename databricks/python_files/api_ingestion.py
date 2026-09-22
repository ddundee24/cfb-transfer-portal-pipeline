import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import IntegerType

## STEP 1: Locate and load the api.env file in Databricks Workspace ##

current_dir = os.getcwd()
env_path = os.path.join(current_dir, "api.env") ## grab api path

if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path) ## load api path from step above
    print(f"Successfully loaded environment variables from: {env_path}") ## successful run
else:
    print(f"Warning: Configuration file 'api.env' was not found at: {env_path}") ## errored run

## STEP 2: Extract and validate environment variables ##

api_key = os.getenv("token") ## set api_key to the key requested
base_url = os.getenv("base_url") ## base url for collegefootballdata.com

if not api_key:
    raise ValueError(
        "Variable 'token' is missing or empty. "
    ) ## error on api key (token)

if not base_url:
    raise ValueError(
        "Variable 'base_url' is missing or empty. "
    ) ## error on base url

print("Environment setup complete. Ready to connect to API base URL:", base_url) ##successful run

## Completed API setup

## Step 3: Extract Player Data including usage, transfer, and recruiting data to _01_raw_bronze schema ## 

spark = SparkSession.builder.getOrCreate() ## starting spark session

## Step 3A: Extract Player usage data from 2020 to 2026

endpoint = f"{base_url}/player/usage" ## api reference page target
headers = {
    "Authorization": f"Bearer {api_key}",
    "Accept": "application/json"
}

# Define the range of seasons
years = list(range(2020, 2027))  # [2020, 2021, 2022, 2023, 2024, 2025, 2026] since we want to include 2026
target_table = "cfb_transfer_portal_pipeline._01_raw_bronze.player_usage" ## catalog.schema.[any_name_for_table]

total_ingested = 0 ## set total ingested to 0 to track how many are ingested for error purposes

for year in years: ## for loop to grab the player data for all years wanted
    print(f"Fetching player usage data for season {year}...") ## will display which year is being currently being extracted for tracking purposes
    
    params = {
        "year": year,
        "excludeGarbageTime": "true"
    }

    response = requests.get(endpoint, headers=headers, params=params) ## grab api data from parameters above

    if response.status_code == 200:
        data = response.json()
        
        if not data:
            print(f"No data returned for year {year}. Skipping...") ## error with return on erroring year
            continue

        # Load into Pandas first to normalize structures 
        pdf = pd.json_normalize(data)
        
        # Convert to PySpark DataFrame
        df = spark.createDataFrame(pdf.astype(str))
        
        # Add data extraction tracking columns with explicit IntegerType casting to prevent Delta merge type conflicts
        df_transformed = (
            df \
            .withColumn("season", lit(int(year)).cast(IntegerType())) \
            .withColumn("ingested_at", current_timestamp()) ## lists when data was last loaded into dataframe
        )
        
        # Append to the raw bronze table with dynamic partition overwrite enabled
        df_transformed.write \
            .format("delta") \
            .mode("overwrite") \
            .option("partitionOverwriteMode", "dynamic") \
            .partitionBy("season") \
            .option("mergeSchema", "true") \
            .saveAsTable(target_table)
                
        row_count = df_transformed.count()
        total_ingested += row_count ## from earlier to track progress
        print(f"Successfully ingested {row_count} rows for {year}.") ## use for job checks and QA

    else:
        print(f"Failed to fetch data for {year} [Status Code {response.status_code}]: {response.text}") ## error tracker for years and why to help in troubleshooting

print(f"\nTotal rows ingested across 2020–2026: {total_ingested}") ## print out total rows across all years when successful


## Step 3B: Extract transfer data from 2020 to 2026

endpoint = f"{base_url}/player/portal" ## api reference page target
headers = {
    "Authorization": f"Bearer {api_key}",
    "Accept": "application/json"
}

# Define the range of seasons
years = list(range(2020, 2027))  # [2020, 2021, 2022, 2023, 2024, 2025, 2026] since we want to include 2026
target_table = "cfb_transfer_portal_pipeline._01_raw_bronze.transfer_portal" ## catalog.schema.[any_name_for_table]

total_ingested = 0 ## set total ingested to 0 to track how many are ingested for error purposes

for year in years: ## for loop to grab the transfer data for all years wanted
    print(f"Fetching transfer portal data for season {year}...") ## will display which year is being currently being extracted for tracking purposes
    
    params = {
        "year": year
    }

    response = requests.get(endpoint, headers=headers, params=params) ## grab api data from parameters above

    if response.status_code == 200:
        data = response.json()
        
        if not data:
            print(f"No data returned for year {year}. Skipping...") ## error with return on erroring year
            continue

        # Load into Pandas first to normalize structures 
        pdf = pd.json_normalize(data)
        
        # Convert to PySpark DataFrame
        df = spark.createDataFrame(pdf.astype(str))
        
        # Add data extraction tracking columns with explicit IntegerType casting to prevent Delta merge type conflicts
        df_transformed = (
            df \
            .withColumn("season", lit(int(year)).cast(IntegerType())) \
            .withColumn("ingested_at", current_timestamp()) ## lists when data was last loaded into dataframe
        )
        
        # Append to the raw bronze table with dynamic partition overwrite enabled
        df_transformed.write \
            .format("delta") \
            .mode("overwrite") \
            .option("partitionOverwriteMode", "dynamic") \
            .partitionBy("season") \
            .option("mergeSchema", "true") \
            .saveAsTable(target_table)
                
        row_count = df_transformed.count()
        total_ingested += row_count ## from earlier to track progress
        print(f"Successfully ingested {row_count} rows for {year}.") ## use for job checks and QA

    else:
        print(f"Failed to fetch data for {year} [Status Code {response.status_code}]: {response.text}") ## error tracker for years and why to help in troubleshooting

print(f"\nTotal rows ingested across 2020–2026: {total_ingested}") ## print out total rows across all years when successful


## Step 3C: Extract recruiting data from 2016 to 2026. Start in 2016 to include data on upper classman transfers.

endpoint = f"{base_url}/recruiting/players" ## api reference page target
headers = {
    "Authorization": f"Bearer {api_key}",
    "Accept": "application/json"
}

# Define the range of seasons
years = list(range(2016, 2027))  # [2020, 2021, 2022, 2023, 2024, 2025, 2026] since we want to include 2026
target_table = "cfb_transfer_portal_pipeline._01_raw_bronze.recruiting_players" ## catalog.schema.[any_name_for_table]

total_ingested = 0 ## set total ingested to 0 to track how many are ingested for error purposes

for year in years: ## for loop to grab the recruiting data for all years wanted
    print(f"Fetching recruiting player data for season {year}...") ## will display which year is being currently being extracted for tracking purposes
    
    params = {
        "year": year
    }

    response = requests.get(endpoint, headers=headers, params=params) ## grab api data from parameters above

    if response.status_code == 200:
        data = response.json()
        
        if not data:
            print(f"No data returned for year {year}. Skipping...") ## error with return on erroring year
            continue

        # Load into Pandas first to normalize structures 
        pdf = pd.json_normalize(data)
        
        # Convert to PySpark DataFrame
        df = spark.createDataFrame(pdf.astype(str))
        
        # Add data extraction tracking columns with explicit IntegerType casting to prevent Delta merge type conflicts
        df_transformed = (
            df \
            .withColumn("season", lit(int(year)).cast(IntegerType())) \
            .withColumn("ingested_at", current_timestamp()) ## lists when data was last loaded into dataframe
        )
        
        # Append to the raw bronze table with dynamic partition overwrite enabled
        df_transformed.write \
            .format("delta") \
            .mode("overwrite") \
            .option("partitionOverwriteMode", "dynamic") \
            .partitionBy("season") \
            .option("mergeSchema", "true") \
            .saveAsTable(target_table)
                
        row_count = df_transformed.count()
        total_ingested += row_count ## from earlier to track progress
        print(f"Successfully ingested {row_count} rows for {year}.") ## use for job checks and QA

    else:
        print(f"Failed to fetch data for {year} [Status Code {response.status_code}]: {response.text}") ## error tracker for years and why to help in troubleshooting

print(f"\nTotal rows ingested across 2020–2026: {total_ingested}") ## print out total rows across all years when successful
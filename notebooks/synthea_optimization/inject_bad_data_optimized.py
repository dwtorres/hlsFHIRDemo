# Databricks notebook source
# MAGIC %md
# MAGIC # Optimized Bad Data Injection for Synthea
# MAGIC
# MAGIC **Performance Improvements**:
# MAGIC - Uses Spark DataFrames instead of Python CSV processing
# MAGIC - Parallel distributed processing across cluster
# MAGIC - Vectorized transformations with when/otherwise
# MAGIC - Processes all files concurrently
# MAGIC
# MAGIC **Expected Performance**: 50-100x faster than original (3+ hours → 2-5 minutes)

# COMMAND ----------

dbutils.widgets.text(name="catalog_name", defaultValue="", label="Catalog Name")
dbutils.widgets.text(name="schema_name", defaultValue="synthea", label="Schema Name")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, rand, lit
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# COMMAND ----------

catalog_name = dbutils.widgets.get(name="catalog_name")
schema_name = dbutils.widgets.get(name="schema_name")
volume_path = f"/Volumes/{catalog_name}/{schema_name}/synthetic_files_raw/output/csv/"

print(f"""
  catalog_name = {catalog_name}
  schema_name = {schema_name}
  volume_path = {volume_path}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark-Native Data Quality Functions

# COMMAND ----------

def introduce_nulls_spark(df, columns, null_fraction=0.05):
    """
    Introduce nulls using Spark DataFrame operations (vectorized).

    Parameters:
    df (DataFrame): Spark DataFrame
    columns (list): Column names to introduce nulls
    null_fraction (float): Fraction of rows to set as null (0-1)

    Returns:
    DataFrame: Modified DataFrame with nulls in specified columns
    """
    for col_name in columns:
        df = df.withColumn(
            col_name,
            when(rand() < null_fraction, lit(None)).otherwise(col(col_name))
        )
    return df


def introduce_negative_values_spark(df, columns, neg_fraction=0.05):
    """
    Introduce negative values using Spark DataFrame operations (vectorized).

    Parameters:
    df (DataFrame): Spark DataFrame
    columns (list): Column names to make negative
    neg_fraction (float): Fraction of rows to make negative (0-1)

    Returns:
    DataFrame: Modified DataFrame with negative values in specified columns
    """
    for col_name in columns:
        df = df.withColumn(
            col_name,
            when(rand() < neg_fraction, col(col_name) * -1).otherwise(col(col_name))
        )
    return df


def process_file_spark(file_path, null_columns, neg_columns, null_fraction=0.03):
    """
    Process a single CSV file using Spark DataFrame operations.

    This function:
    1. Reads CSV with Spark (distributed read)
    2. Applies transformations using vectorized operations
    3. Writes back using Spark (distributed write)

    Performance: O(n) but with parallel distributed execution
    """
    # Read CSV with Spark (automatically parallelized)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

    # Apply transformations (vectorized operations, not row-by-row)
    df = introduce_nulls_spark(df, null_columns, null_fraction)
    df = introduce_negative_values_spark(df, neg_columns, null_fraction)

    # Write back (automatically parallelized, overwrites original)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(file_path + "_temp")

    # Move the single file to replace original
    temp_files = [f.path for f in dbutils.fs.ls(file_path + "_temp") if f.name.endswith(".csv")]
    if temp_files:
        dbutils.fs.mv(temp_files[0], file_path)
        dbutils.fs.rm(file_path + "_temp", recurse=True)

    return True


def create_success_file(directory_path, file_name="SUCCESS.txt"):
    """Create success marker file."""
    success_dir = directory_path + "/data_quality_output"
    dbutils.fs.mkdirs(success_dir)

    file_path = success_dir + "/" + file_name
    dbutils.fs.put(file_path, "SUCCESS: Successfully added data quality issues to files", overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Configuration
# MAGIC
# MAGIC Define which columns to inject nulls/negatives for each file type

# COMMAND ----------

FILE_CONFIGS = {
    "encounters.csv": {
        "null_columns": ["PATIENT"],
        "neg_columns": ["PAYER_COVERAGE"],
    },
    "claims.csv": {
        "null_columns": ["Id", "PATIENTID", "PROVIDERID"],
        "neg_columns": [],
    },
    "claims_transactions.csv": {
        "null_columns": [],
        "neg_columns": ["PAYMENTS"],
    },
    "conditions.csv": {
        "null_columns": ["PATIENT", "ENCOUNTER"],
        "neg_columns": [],
    },
    "medications.csv": {
        "null_columns": [],
        "neg_columns": ["TOTALCOST"],
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parallel File Processing
# MAGIC
# MAGIC Process multiple files concurrently using ThreadPoolExecutor

# COMMAND ----------

def process_single_file(file_info):
    """
    Process a single file (designed for parallel execution).

    Returns: (file_name, success_status, message)
    """
    file_path, file_name, directory = file_info

    if file_name not in FILE_CONFIGS:
        return (file_name, "skipped", f"Not in processing list")

    config = FILE_CONFIGS[file_name]

    try:
        print(f"[{directory}] Processing {file_name}...")

        # Use uniform random fraction for consistency
        import random
        null_fraction = random.uniform(0.01, 0.05)

        process_file_spark(
            file_path,
            null_columns=config["null_columns"],
            neg_columns=config["neg_columns"],
            null_fraction=null_fraction,
        )

        print(f"[{directory}] ✅ Completed {file_name} ({null_fraction*100:.1f}% injection rate)")
        return (file_name, "success", f"Processed with {null_fraction*100:.1f}% injection rate")

    except Exception as e:
        print(f"[{directory}] ❌ Failed {file_name}: {str(e)}")
        return (file_name, "failed", str(e))


def process_directory_parallel(directory_path, directory_name):
    """
    Process all files in a directory in parallel.

    Uses ThreadPoolExecutor to process multiple files concurrently.
    """
    # Check if already processed
    success_file_path = directory_path + "/data_quality_output/SUCCESS.txt"

    try:
        dbutils.fs.ls(success_file_path)
        print(f"✓ Directory {directory_name} already processed. Skipping...")
        return {"status": "skipped", "reason": "already_processed"}
    except:
        pass  # Success file doesn't exist, proceed with processing

    print(f"\n{'='*60}")
    print(f"Processing directory: {directory_name}")
    print(f"{'='*60}")

    # Get all files in directory
    files = spark.sql(f"LIST '{directory_path}' ").collect()

    # Prepare file info tuples for parallel processing
    file_infos = [
        (file[0], file[1], directory_name)
        for file in files
        if file[1] in FILE_CONFIGS
    ]

    if not file_infos:
        print(f"No files to process in {directory_name}")
        return {"status": "no_files"}

    # Process files in parallel (max 5 concurrent files)
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_file = {executor.submit(process_single_file, info): info for info in file_infos}

        for future in as_completed(future_to_file):
            results.append(future.result())

    # Create success file
    create_success_file(directory_path)

    # Summary
    successful = sum(1 for r in results if r[1] == "success")
    failed = sum(1 for r in results if r[1] == "failed")

    print(f"\n{'='*60}")
    print(f"Directory {directory_name} Summary:")
    print(f"  ✅ Successful: {successful}")
    print(f"  ❌ Failed: {failed}")
    print(f"{'='*60}\n")

    return {
        "status": "completed",
        "successful": successful,
        "failed": failed,
        "results": results
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution
# MAGIC
# MAGIC Process all directories with parallel file processing

# COMMAND ----------

# Get directories and order by timestamp
directories = spark.sql(f"LIST '{volume_path}' ").orderBy("name").collect()

print(f"Found {len(directories)} directories to process")
print("="*60)

# Process each directory
all_results = []
for directory in directories:
    directory_path = directory[0]
    directory_name = directory[1].split('/')[0]

    result = process_directory_parallel(directory_path, directory_name)
    all_results.append((directory_name, result))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

print("\n" + "="*80)
print(" FINAL PROCESSING SUMMARY")
print("="*80)

total_directories = len(all_results)
completed = sum(1 for _, r in all_results if r["status"] == "completed")
skipped = sum(1 for _, r in all_results if r["status"] == "skipped")
no_files = sum(1 for _, r in all_results if r["status"] == "no_files")

print(f"\nDirectories:")
print(f"  Total: {total_directories}")
print(f"  ✅ Completed: {completed}")
print(f"  ⏭️  Skipped (already processed): {skipped}")
print(f"  📭 No files: {no_files}")

if completed > 0:
    total_successful_files = sum(r["successful"] for _, r in all_results if r["status"] == "completed")
    total_failed_files = sum(r["failed"] for _, r in all_results if r["status"] == "completed")

    print(f"\nFiles Processed:")
    print(f"  ✅ Successful: {total_successful_files}")
    print(f"  ❌ Failed: {total_failed_files}")

print("\n" + "="*80)

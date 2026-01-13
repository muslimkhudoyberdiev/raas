from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
import argparse
import sys

class PeopleDataQualityValidator:
    def __init__(self, spark_session, run_id, pipeline_name):
        self.spark = spark_session
        self.run_id = run_id
        self.pipeline_name = pipeline_name
        self.results = []
 
    def log_result(self, table_name, check_name, status, message, invalid_count=0):
        result = {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "test_timestamp": datetime.now(),
            "table_name": table_name,
            "test_check": check_name,
            "record_count": invalid_count,
            "message": message,
            "status": status
        }
        self.results.append(result)
        print(f"[{status}] {table_name} - {check_name}: {message} (Count: {invalid_count})")
 
    def check_nulls(self, df, table_name, columns):
        """Checks for null values in critical columns."""
        print(f"Running Null checks on {table_name}...")
        for column in columns:
            if column not in df.columns:
                self.log_result(table_name, f"Null Check - {column}", "ERROR", f"Column {column} not found")
                continue
            
            null_count = df.filter(col(column).isNull() | (col(column) == "")).count()
            if null_count > 0:
                self.log_result(table_name, f"Null Check - {column}", "FAIL", f"Found {null_count} null/empty values", null_count)
            else:
                self.log_result(table_name, f"Null Check - {column}", "PASS", "No null values found")
 
    def check_uniqueness(self, df, table_name, key_columns):
        """Checks for uniqueness of primary key columns."""
        print(f"Running Uniqueness checks on {table_name}...")
        
        # Ensure key_columns is a list
        if isinstance(key_columns, str):
            key_columns = [key_columns]
 
        # Verify columns exist
        missing_cols = [c for c in key_columns if c not in df.columns]
        if missing_cols:
             self.log_result(table_name, f"Uniqueness Check - {key_columns}", "ERROR", f"Columns not found: {missing_cols}")
             return
 
        window_spec = df.groupBy(key_columns).count()
        duplicate_count = window_spec.filter(col("count") > 1).count()
        
        if duplicate_count > 0:
            self.log_result(table_name, f"Uniqueness Check - {key_columns}", "FAIL", f"Found {duplicate_count} duplicate keys", duplicate_count)
        else:
            self.log_result(table_name, f"Uniqueness Check - {key_columns}", "PASS", "Unique keys valid")

    def save_logs_to_table(self, lakehouse_name=None):
        """Saves validation results to Delta tables dynamically based on the schema of the validated table."""
        if not self.results:
            print("No results to save.")
            return
 
        schema = StructType([
            StructField("run_id", StringType(), True),
            StructField("pipeline_name", StringType(), True),
            StructField("test_timestamp", TimestampType(), True),
            StructField("table_name", StringType(), True),
            StructField("test_check", StringType(), True),
            StructField("record_count", IntegerType(), True),
            StructField("message", StringType(), True),
            StructField("status", StringType(), True)
        ])
        
        # Group results by target schema/table for logging
        # We assume the log table is {Schema}.AuditLogs
        logs_by_schema = {}
        
        prefix = f"{lakehouse_name}." if lakehouse_name else ""
 
        for res in self.results:
            full_table_name = res["table_name"]
            
            # Attempt to parse schema from table name
            # Format usually: [Lakehouse.]Schema.Table
            parts = full_table_name.split('.')
            target_schema = None
            
            # Heuristic to find schema
            if len(parts) >= 2:
                # The table name is the last part, the schema is the second to last
                target_schema = parts[-2]
            else:
                print(f"Warning: Could not extract schema from {full_table_name}. Skipping.")
                continue
            
            # Construct the log table path
            log_table = f"{prefix}{target_schema}.AuditLogs"
            
            if log_table not in logs_by_schema:
                logs_by_schema[log_table] = []
            
            logs_by_schema[log_table].append(res)
            
        # Save batches
        for log_table_name, records in logs_by_schema.items():
            print(f"Saving {len(records)} logs to {log_table_name}...")
            try:
                df_log = self.spark.createDataFrame(records, schema)
                df_log.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(log_table_name)
                print(f"Successfully saved to {log_table_name}.")
            except Exception as e:
                print(f"Error saving to {log_table_name}: {str(e)}")

def get_spark_session():
    try:
        return SparkSession.builder.appName("People Data Quality").enableHiveSupport().getOrCreate()
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        sys.exit(1)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Run Dynamic Data Quality Checks')
    parser.add_argument('--schema', type=str, required=True, help='Database Schema Name')
    parser.add_argument('--table', type=str, required=True, help='Table Name')
    parser.add_argument('--columns', type=str, required=True, help='Comma separated list of columns to check')
    parser.add_argument('--run_id', type=str, required=False, help='Pipeline Run ID')
    
    # If arguments are passed via sys.argv (standard python script usage)
    if len(sys.argv) > 1:
        return parser.parse_args()
    
    # If running in a notebook-like environment or no args passed, return None or handle differently
    # For this script, we assume CLI usage.
    return None

def main():
    spark = get_spark_session()
    
    # Try getting arguments from CLI
    args = parse_arguments()
    
    # Fallback/Handling for different runtime environments (like passing args as variables)
    if args:
        p_schema = args.schema
        p_table = args.table
        p_columns = args.columns
        p_run_id = args.run_id
    else:
        # If no CLI args, we could try to look for them in Spark conf or environment variables
        # This part mimics the notebook logic where variables might just exist
        # For a pure .py script, this is less common unless using specific runners
        print("No arguments provided via CLI. Exiting.")
        sys.exit(1)

    if not p_run_id:
        p_run_id = spark.conf.get("spark.fabric.runId", "MANUAL_RUN_" + datetime.now().strftime("%Y%m%d%H%M%S"))

    PIPELINE_NAME = "Dynamic_Data_Quality_Check"
    validator = PeopleDataQualityValidator(spark, p_run_id, PIPELINE_NAME)

    full_table_name = f"{p_schema}.{p_table}"
    columns_list = [c.strip() for c in p_columns.split(',')]

    print(f"\n--- Starting checks for {full_table_name} ---")
    print(f"Run ID: {p_run_id}")
    print(f"Columns: {columns_list}")

    try:
        df = spark.table(full_table_name)
        
        # 1. Null Checks
        validator.check_nulls(df, full_table_name, columns_list)
        
        # 2. Uniqueness Checks
        validator.check_uniqueness(df, full_table_name, columns_list)

    except Exception as e:
        validator.log_result(full_table_name, "Load/Process Table", "ERROR", str(e))
        print(f"Error processing table: {e}")

    # Save Logs (Dynamically to {Schema}.AuditLogs)
    LAKEHOUSE_NAME = None 
    validator.save_logs_to_table(lakehouse_name=LAKEHOUSE_NAME)

    # Display Summary
    print("\n" + "="*50)
    print("DATA QUALITY TEST SUMMARY")
    print("="*50)

    if validator.results:
        # Simple print for logs since show() might not look great in standard stdout logs depending on width
        for res in validator.results:
            print(f"{res['status']}: {res['test_check']} - {res['message']}")
        
        failed_checks = [r for r in validator.results if r['status'] in ['FAIL', 'ERROR']]
        if failed_checks:
            print(f"\nWARNING: {len(failed_checks)} data quality checks failed.")
            sys.exit(1) # Optional: Fail the job if checks fail
        else:
            print("\nSUCCESS: All data quality checks passed.")
    else:
        print("No checks run or no results.")

if __name__ == "__main__":
    main()

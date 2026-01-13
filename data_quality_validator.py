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

    def save_logs_to_table(self, lakehouse_name=None, target_table_name=None):
        """Saves validation results to Delta tables.
        
        If target_table_name is provided, all logs are saved to that table.
        Otherwise, logs are saved dynamically based on the schema of the validated table ({lakehouse}.{schema}.AuditLogs).
        """
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
        
        # If a specific target table is provided, save all results there
        if target_table_name:
            print(f"Saving {len(self.results)} logs to {target_table_name}...")
            try:
                df_log = self.spark.createDataFrame(self.results, schema)
                df_log.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table_name)
                print(f"Successfully saved to {target_table_name}.")
            except Exception as e:
                print(f"Error saving to {target_table_name}: {str(e)}")
            return

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
    parser.add_argument('--schema', type=str, required=False, help='Database Schema Name')
    parser.add_argument('--table', type=str, required=False, help='Table Name')
    parser.add_argument('--columns', type=str, required=False, help='Comma separated list of columns to check')
    parser.add_argument('--rules', type=str, required=False, help='JSON rules for checks')
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
        schema = args.schema
        table = args.table
        columns = args.columns
        rules = args.rules
        run_id = args.run_id
    else:
        # Check if variables exist in the global scope (for notebook usage)
        # In a standard python script, these won't "just exist" unless injected. 
        # But we can look for them if the user modifies the script to define them at the top level
        # or expects us to use the specific hardcoded values requested.
        
        # Since the user specifically said "i have variables with parameters" and provided values,
        # I will inject these defaults here for when no CLI args are present.
        
        # Default/Hardcoded values for manual testing/notebook usage
        schema = globals().get('schema', "Worker_HR")
        table = globals().get('table', "Education_1")
        run_id = globals().get('run_id', "MANUAL_TEST_RUN")
        rules_default = """
{
    "null_cols": ["Colleague_ID"],
    "unique_keys": [
        ["Colleague_ID","Degree_ID"]
    ]
}
"""
        rules = globals().get('rules', rules_default)
        columns = globals().get('columns', None)

        print("Using default/global variables since no CLI arguments provided.")


    if not run_id or run_id == "MANUAL_TEST_RUN":
        run_id = spark.conf.get("spark.fabric.runId", "MANUAL_RUN_" + datetime.now().strftime("%Y%m%d%H%M%S"))
    
    schemas = [s.strip() for s in schema.split(',')]
    
    # 2. Parse Rules
    null_check_cols = []
    unique_check_combinations = []
    
    try:
        clean_rules = rules.strip() if rules else ""
        if clean_rules.startswith('"""') and clean_rules.endswith('"""'):
            clean_rules = clean_rules[3:-3].strip()
        elif clean_rules.startswith('"') and clean_rules.endswith('"') and not clean_rules.startswith("{"):
             clean_rules = clean_rules.strip('"').strip("'")
    
        if clean_rules and clean_rules.startswith("{"):
            print("Parsing checks from JSON rules...")
            import json
            config = json.loads(clean_rules)
            null_check_cols = config.get("null_cols", [])
            unique_check_combinations = config.get("unique_keys", [])
        elif columns:
            print("Parsing checks from CSV columns list...")
            cols_list = [c.strip() for c in columns.split(',')]
            null_check_cols = cols_list
            unique_check_combinations = [cols_list]
    except Exception as e:
        print(f"Error parsing rules: {e}")
    
    # 3. Iterate over Schemas
    for current_schema in schemas:
        # Construct table name
        full_table_name = f"{current_schema}.{table}"
        PIPELINE_NAME = "Dynamic_Data_Quality_Check"
        
        validator = PeopleDataQualityValidator(spark, run_id, PIPELINE_NAME)
        
        print(f"\n--- Starting Quality Checks for {full_table_name} ---")
        
        try:
            if null_check_cols or unique_check_combinations:
                # spark.table is usually case-insensitive in resolution
                df = spark.table(full_table_name)
                
                if null_check_cols:
                    validator.check_nulls(df, full_table_name, null_check_cols)
                
                if unique_check_combinations:
                    # check_uniqueness expects key_columns as a single list, not a list of lists 
                    # based on original implementation which took `columns_list`.
                    # But the new logic prepares `unique_check_combinations` as a list of lists?
                    # Let's check original implementation:
                    # `def check_uniqueness(self, df, table_name, key_columns):`
                    # `    if isinstance(key_columns, str): key_columns = [key_columns]`
                    # `    window_spec = df.groupBy(key_columns).count()`
                    # So it expects a list of columns for ONE unique constraint.
                    
                    # The snippet provided by user implies `unique_check_combinations` might be multiple constraints?
                    # `unique_check_combinations = config.get("unique_keys", [])` -> could be [[col1], [col2, col3]]?
                    # If so, we need to iterate.
                    
                    # If it came from `columns` csv: `unique_check_combinations = [cols_list]` -> [[c1, c2, ...]]
                    
                    for combo in unique_check_combinations:
                        validator.check_uniqueness(df, full_table_name, combo)

            else:
                print("No checks configured to run.")
                
        except Exception as e:
            validator.log_result(full_table_name, "Load/Process Table", "ERROR", str(e))
            print(f"Error processing {full_table_name}: {e}")
        
        # 4. Save Logs
        LAKEHOUSE_NAME = "hbvkj6b5fvzenlsxgtupezx6wq-f5va56hadvsuhlocx4wmlexjv4.datawarehouse.fabric.microsoft.com" 
        validator.save_logs_to_table(lakehouse_name=LAKEHOUSE_NAME)
        
        failed_checks = [r for r in validator.results if r['status'] in ['FAIL', 'ERROR']]
        if failed_checks:
            print(f"WARNING: {len(failed_checks)} checks failed for {current_schema}.")
            sys.exit(1)
        else:
            print(f"SUCCESS: All checks passed for {current_schema}.")

if __name__ == "__main__":
    main()

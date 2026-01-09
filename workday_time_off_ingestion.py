import requests
import base64
import json
import time
import random
from datetime import datetime, timedelta
# from pyspark.sql.functions import lit # Assuming this runs in a Spark environment
# from pyspark.sql import SparkSession # Assuming this runs in a Spark environment

# Placeholder for Spark session and mssparkutils if running locally
try:
    spark
except NameError:
    spark = None

try:
    mssparkutils
except NameError:
    class MockMsSparkUtils:
        class FS:
            def exists(self, path): return False
            def rm(self, path, recursive): pass
            def mkdirs(self, path): pass
            def put(self, path, content, overwrite): pass
        fs = FS()
    mssparkutils = MockMsSparkUtils()

def refresh_workday_access_token(client_id: str, client_secret: str, refresh_token: str, token_endpoint: str) -> str:
    try:
        print("Refreshing Workday access token...")

        raw = f"{client_id}:{client_secret}"
        b64 = base64.b64encode(raw.encode()).decode()

        headers = {
            "Authorization": f"Basic {b64}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        response = requests.post(token_endpoint, headers=headers, data=payload)
        response.raise_for_status()

        access_token = response.json()["access_token"]
        print("Access token successfully refreshed.")
        return access_token

    except Exception as e:
        print("ERROR while refreshing Workday access token:", e)
        raise

def fetch_time_off_report_data(base_endpoint: str, access_token: str, colleague_id: str, date_str: str):
    """
    Fetches the time off report for a specific colleague and date.
    Endpoint: .../CRI_INT0137_USA_Datahub_Timeoffs
    """
    try:
        # Construct the full URL if base_endpoint doesn't include the report path
        # User provided: https://wd3-impl-services1.workday.com/ccx/service/customreport2/nrf3/INT0137_USA_HCM_Datahub_Absence_ISU/CRI_INT0137_USA_Datahub_Timeoffs
        url = base_endpoint 
        
        # Format date as needed by Workday: YYYY-MM-DD-08:00 based on example
        # Example input: 2026-01-06
        # Example output param: 2026-01-06-08:00
        formatted_date_param = f"{date_str}-08:00"
        
        params = {
            "promptDate1": formatted_date_param,
            "Include_Terminated_Workers": "0",
            "Colleague_ID": colleague_id,
            "date": formatted_date_param,
            "format": "json"
        }

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

        # print(f"Requesting Time Off for {colleague_id} on {date_str}...")
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            # Return Report_Entry list or empty list if not found
            return data.get("Report_Entry", [])
        else:
            print(f"Failed to fetch for {colleague_id} on {date_str}: {response.status_code} - {response.text}")
            return []

    except Exception as e:
        print(f"ERROR while fetching Time Off report for {colleague_id} on {date_str}: {e}")
        # We might not want to raise here to continue the loop, but logging is essential
        return []

def get_worker_details_mock():
    """
    Mock function to simulate getting worker details (Colleague ID and Hire Date) from a table.
    In real implementation, this would query the worker_details Delta table.
    """
    return [
        {"colleague_id": "50454", "hire_date": "2026-01-01"},
        # Add more workers here
    ]

def ingest_time_off_history(
    client_id: str, 
    client_secret: str, 
    refresh_token: str, 
    token_endpoint: str,
    report_endpoint: str,
    raw_current_path: str,
    raw_archive_path: str
):
    print("=== Workday Time Off History Ingestion Started ===")

    # 1. Get Token
    access_token = refresh_workday_access_token(
        client_id, client_secret, refresh_token, token_endpoint
    )

    # 2. Get Workers
    workers = get_worker_details_mock() # Replace with actual DB read
    
    all_time_off_data = []

    today = datetime.now().date()

    for worker in workers:
        colleague_id = worker["colleague_id"]
        hire_date_str = worker["hire_date"]
        
        try:
            current_date = datetime.strptime(hire_date_str, "%Y-%m-%d").date()
        except ValueError:
            print(f"Invalid hire date format for {colleague_id}: {hire_date_str}")
            continue

        print(f"Processing Colleague: {colleague_id} from {current_date} to {today}")

        # Loop from hire_date to today
        while current_date <= today:
            date_str = current_date.strftime("%Y-%m-%d")
            
            # Fetch data for this day
            entries = fetch_time_off_report_data(
                report_endpoint, access_token, colleague_id, date_str
            )
            
            if entries:
                all_time_off_data.extend(entries)
            
            # Move to next day
            current_date += timedelta(days=1)
            
            # Optional: Sleep to avoid rate limiting
            # time.sleep(0.1)

    # 3. Write results to Lakehouse
    if all_time_off_data:
        write_workday_raw_snapshot_json(
            report_data=all_time_off_data,
            report_name="TimeOff_History",
            raw_current_path=raw_current_path,
            raw_archive_path=raw_archive_path
        )
    else:
        print("No time off data found for the processed workers.")

    print("=== Workday Time Off History Ingestion Finished ===")

def write_workday_raw_snapshot_json(
    report_data,
    report_name: str,
    raw_current_path: str,
    raw_archive_path: str
):
    from pyspark.sql.functions import lit # Import locally to avoid top-level error if pyspark missing
    
    if isinstance(report_data, dict):
        report_data = [report_data]
 
    if not isinstance(report_data, list):
        raise Exception(f"Unexpected report_data type: {type(report_data)}")
        
    print(f"Write data to lakehouse, Length: {len(report_data)}")
    if len(report_data) == 0:
        return
 
    now = datetime.utcnow()
    try:
        # 1. Update Current Snapshot (Delta)
        rdd = spark.sparkContext.parallelize([json.dumps(row) for row in report_data])
        
        # FIX 1: samplingRatio=1.0 ensures all rows are scanned for schema inference
        df_current = spark.read.option("samplingRatio", 1.0).json(rdd)
        
        # Check if we still have the correct count
        if df_current.count() != len(report_data):
            print(f"WARNING: Row count mismatch! Input: {len(report_data)}, DataFrame: {df_current.count()}")
 
        df_current = df_current.withColumn("ingest_time", lit(now))
        current_path = f"{raw_current_path}/{report_name}"
        
        # FIX 2: Retry logic for ConcurrentAppendException
        max_retries = 5
        for attempt in range(max_retries):
            try:
                df_current.write \
                    .format('delta')\
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .save(current_path)
                print(f"Updated current raw snapshot (Delta) → {current_path}")
                break # Success! Exit the retry loop
            except Exception as e:
                # Check for Delta concurrency errors
                error_msg = str(e)
                if "ConcurrentAppendException" in error_msg or "DeltaConcurrentModificationException" in error_msg:
                    if attempt < max_retries - 1:
                        # Wait randomly between 2 and 10 seconds to avoid thundering herd
                        wait_time = random.uniform(2, 10)
                        print(f"⚠️ Concurrent write detected. Retrying in {wait_time:.2f}s... (Attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        print("❌ Max retries reached for Delta write.")
                        raise e # Fail after all retries
                else:
                    raise e # Not a concurrency error, fail immediately
 
        # 2. Archive Snapshot (Single JSON inside a folder)
        base_archive_dir = f"{raw_archive_path}/Y={now.year}/M={now.month}/D={now.day}"
        specific_archive_dir = f"{base_archive_dir}/{report_name}"
        archive_filename = f"{report_name}.json"
        archive_path = f"{specific_archive_dir}/{archive_filename}"
        
        old_parquet_path = f"{base_archive_dir}/{report_name}.parquet"
        if mssparkutils.fs.exists(old_parquet_path):
            mssparkutils.fs.rm(old_parquet_path, True)
            print(f"Removed old parquet archive: {old_parquet_path}")
 
        archive_data = []
        str_now = now.isoformat()
        for row in report_data:
            new_row = row.copy()
            new_row['ingest_time'] = str_now
            archive_data.append(new_row)
            
        json_content = json.dumps(archive_data)
 
        mssparkutils.fs.mkdirs(specific_archive_dir)
        mssparkutils.fs.put(archive_path, json_content, True)
        print(f"Archived raw snapshot (Single JSON) → {archive_path}")
 
    except NameError:
        print("Spark session not found. Skipping write step (simulated).")
    except Exception as e:
        print(f"Error in write step: {e}")
        # raise e # Commented out to prevent crash in non-spark env

# Example Usage Configuration
if __name__ == "__main__":
    CLIENT_ID = "your_client_id"
    CLIENT_SECRET = "your_client_secret"
    REFRESH_TOKEN = "your_refresh_token"
    TOKEN_ENDPOINT = "https://wd3-impl-services1.workday.com/ccx/oauth2/nrf3/token"
    
    # Report Endpoint
    TIMEOFF_REPORT_ENDPOINT = "https://wd3-impl-services1.workday.com/ccx/service/customreport2/nrf3/INT0137_USA_HCM_Datahub_Absence_ISU/CRI_INT0137_USA_Datahub_Timeoffs"
    
    RAW_CURRENT_PATH = "/lakehouse/default/Files/raw/current"
    RAW_ARCHIVE_PATH = "/lakehouse/default/Files/raw/archive"
    
    ingest_time_off_history(
        CLIENT_ID, 
        CLIENT_SECRET, 
        REFRESH_TOKEN, 
        TOKEN_ENDPOINT, 
        TIMEOFF_REPORT_ENDPOINT,
        RAW_CURRENT_PATH,
        RAW_ARCHIVE_PATH
    )

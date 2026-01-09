import requests
import base64
import json
import time
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        url = base_endpoint 
        
        # Format date as needed by Workday: YYYY-MM-DD-08:00 based on example
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
            return data.get("Report_Entry", [])
        else:
            print(f"Failed to fetch for {colleague_id} on {date_str}: {response.status_code} - {response.text}")
            return []

    except Exception as e:
        print(f"ERROR while fetching Time Off report for {colleague_id} on {date_str}: {e}")
        return []

def get_worker_details_spark():
    """
    Query the Lakehouse using Spark SQL to get distinct colleagueId.
    """
    try:
        query = """
            SELECT distinct colleagueId 
            FROM US_IT_HRIS_LH_L0_LakeHouse.workday_batch_worker_details
            WHERE colleagueId IS NOT NULL
        """
        print("Executing Spark SQL query to get worker details...")
        df = spark.sql(query)
        # Collect results to a list of dictionaries [Row(colleagueId='...'), ...]
        workers = [row.asDict() for row in df.collect()]
        print(f"Found {len(workers)} workers to process.")
        return workers
    except NameError:
        print("Spark session not available locally. Returning mock data.")
        return [{"colleagueId": "50454"}, {"colleagueId": "61783"}] 
    except Exception as e:
        print(f"Error executing Spark query: {e}")
        return []

def process_single_worker(worker, start_date, today, report_endpoint, access_token):
    """
    Helper function to process a single worker's entire date range.
    Returns a list of raw time off entries for this worker.
    """
    colleague_id = worker.get("colleagueId")
    if not colleague_id:
        return []

    worker_entries = []
    current_date = start_date
    
    # Iterate through days for this worker
    while current_date <= today:
        date_str = current_date.strftime("%Y-%m-%d")
        
        entries = fetch_time_off_report_data(
            report_endpoint, access_token, colleague_id, date_str
        )
        
        if entries:
            # Append raw entries directly without flattening
            worker_entries.extend(entries)
        
        current_date += timedelta(days=1)
        
    return worker_entries

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

    # 2. Get Workers (from Spark SQL)
    workers = get_worker_details_spark()
    
    all_time_off_data = []
    
    start_date = datetime(2026, 1, 1).date()
    today = datetime.now().date()
    
    # 3. Threading Implementation
    MAX_WORKERS = 10 
    
    print(f"Starting threaded processing with {MAX_WORKERS} threads for {len(workers)} workers...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_worker = {
            executor.submit(
                process_single_worker, 
                worker, 
                start_date, 
                today, 
                report_endpoint, 
                access_token
            ): worker 
            for worker in workers
        }
        
        completed_count = 0
        total_workers = len(workers)
        
        for future in as_completed(future_to_worker):
            completed_count += 1
            worker = future_to_worker[future]
            colleague_id = worker.get("colleagueId")
            
            try:
                data = future.result()
                if data:
                    all_time_off_data.extend(data)
                
                if completed_count % 10 == 0:
                    print(f"Progress: {completed_count}/{total_workers} workers processed.")
                    
            except Exception as exc:
                print(f"Worker {colleague_id} generated an exception: {exc}")

    # 4. Write ALL results to Lakehouse (Single File)
    if all_time_off_data:
        write_workday_raw_snapshot_json(
            report_data=all_time_off_data,
            report_name="TimeOff_History_Full_Load",
            raw_current_path=raw_current_path,
            raw_archive_path=raw_archive_path
        )
    else:
        print("No time off data found for any workers.")

    print("=== Workday Time Off History Ingestion Finished ===")

def write_workday_raw_snapshot_json(
    report_data,
    report_name: str,
    raw_current_path: str,
    raw_archive_path: str
):
    from pyspark.sql.functions import lit
    
    if isinstance(report_data, dict):
        report_data = [report_data]
 
    print(f"Writing data to lakehouse, Total Records: {len(report_data)}")
    if len(report_data) == 0:
        return
 
    now = datetime.utcnow()
    try:
        # 1. Update Current Snapshot (Delta)
        rdd = spark.sparkContext.parallelize([json.dumps(row) for row in report_data])
        
        df_current = spark.read.option("samplingRatio", 1.0).json(rdd)
        
        df_current = df_current.withColumn("ingest_time", lit(now))
        current_path = f"{raw_current_path}/{report_name}"
        
        df_current.write \
            .format('delta')\
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(current_path)
        print(f"Updated current raw snapshot (Delta) → {current_path}")
 
        # 2. Archive Snapshot (Single JSON inside a folder)
        base_archive_dir = f"{raw_archive_path}/Y={now.year}/M={now.month}/D={now.day}"
        specific_archive_dir = f"{base_archive_dir}/{report_name}"
        archive_filename = f"{report_name}.json"
        archive_path = f"{specific_archive_dir}/{archive_filename}"
        
        # Clean up old parquet if exists
        old_parquet_path = f"{base_archive_dir}/{report_name}.parquet"
        if mssparkutils.fs.exists(old_parquet_path):
            mssparkutils.fs.rm(old_parquet_path, True)
 
        # Archive as Single JSON
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
        raise e

if __name__ == "__main__":
    CLIENT_ID = "your_client_id"
    CLIENT_SECRET = "your_client_secret"
    REFRESH_TOKEN = "your_refresh_token"
    TOKEN_ENDPOINT = "https://wd3-impl-services1.workday.com/ccx/oauth2/nrf3/token"
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

import requests
import base64
import json
import time
import random
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
        logger.info("Refreshing Workday access token...")

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

        response = requests.post(token_endpoint, headers=headers, data=payload, timeout=30)
        response.raise_for_status()

        access_token = response.json()["access_token"]
        logger.info("Access token successfully refreshed.")
        return access_token

    except Exception as e:
        logger.error(f"ERROR while refreshing Workday access token: {e}")
        raise

def fetch_time_off_report_data(base_endpoint: str, access_token: str, colleague_id: str, prompt_date: str, work_date: str):
    """
    Fetches the time off report for a specific colleague and date parameters.
    Endpoint: .../CRI_INT0137_USA_Datahub_Timeoffs
    """
    try:
        url = base_endpoint 
        
        # Helper to ensure string format
        def fmt_date(d):
            if isinstance(d, (datetime,)):
                return d.strftime("%Y-%m-%d")
            return str(d).split('T')[0] 

        prompt_date_str = fmt_date(prompt_date)
        work_date_str = fmt_date(work_date)

        formatted_prompt_date = f"{prompt_date_str}-08:00"
        formatted_work_date = f"{work_date_str}-08:00"
        
        params = {
            "promptDate1": formatted_prompt_date,
            "Include_Terminated_Workers": "0",
            "Colleague_ID": colleague_id,
            "date": formatted_work_date,
            "format": "json"
        }

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("Report_Entry", [])
        else:
            logger.error(f"Failed to fetch for {colleague_id}: {response.status_code} - {response.text}")
            return []

    except requests.exceptions.Timeout:
        logger.error(f"TIMEOUT fetching Time Off report for {colleague_id}")
        return []
    except Exception as e:
        logger.error(f"ERROR while fetching Time Off report for {colleague_id}: {e}")
        return []

def post_time_off_update(access_token: str, worker_id: str, time_off_entry_wid: str, date_val: str, quantity: str, dry_run: bool = False):
    """
    Sends a POST request to the Absence Management API and returns the parsed result.
    If dry_run is True, returns a mock success response.
    """
    url = f"https://wd3-impl-services1.workday.com/ccx/api/absenceManagement/v3/nrf3/workers/{worker_id}/requestTimeOff"

    if "T" not in str(date_val):
        formatted_date = f"{date_val}T08:00:00.000Z"
    else:
        formatted_date = date_val

    payload_dict = {
        "days": [
            {
                "dailyQuantity": str(quantity),
                "comment": "INT0137",
                "timeOffType": {
                    "descriptor": "Vacation Time Off",
                    "id": time_off_entry_wid 
                },
                "date": formatted_date
            }
        ]
    }
    
    payload = json.dumps(payload_dict)
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    timestamp = datetime.utcnow().isoformat()

    if dry_run:
        logger.info(f"[DRY RUN] Posting time off for {worker_id} on {date_val} with Qty {quantity}")
        return {
            "status": "DRY_RUN",
            "worker_id": worker_id,
            "date": date_val,
            "timestamp": timestamp,
            "payload": payload_dict
        }

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=30)
        
        log_entry = {
            "worker_id": worker_id,
            "request_date": date_val,
            "request_timestamp": timestamp,
            "http_status": response.status_code,
            "response_text": response.text,
            "success": False
        }

        if response.status_code in (200, 201):
            try:
                resp_json = response.json()
                # Parse specific fields
                days_list = resp_json.get("days", [])
                day_entry = days_list[0] if days_list else {}
                
                log_entry.update({
                    "success": True,
                    "id": day_entry.get("id"),
                    "descriptor": day_entry.get("descriptor"),
                    "comment": day_entry.get("comment"),
                    "timeOffType_id": day_entry.get("timeOffType", {}).get("id"),
                    "timeOffType_descriptor": day_entry.get("timeOffType", {}).get("descriptor"),
                    "date": day_entry.get("date"),
                    "dailyQuantity": day_entry.get("dailyQuantity"),
                    "transactionStatus": resp_json.get("businessProcessParameters", {}).get("transactionStatus", {}).get("descriptor")
                })
                logger.info(f"Successfully posted for {worker_id}. Transaction: {log_entry.get('transactionStatus')}")
                
            except Exception as parse_err:
                logger.error(f"Error parsing response for {worker_id}: {parse_err}")
                log_entry["parse_error"] = str(parse_err)
        else:
            logger.error(f"Failed to post for {worker_id}: {response.status_code} - {response.text}")
            
        return log_entry

    except Exception as e:
        logger.error(f"ERROR while posting for {worker_id}: {e}")
        return {
            "worker_id": worker_id,
            "request_date": date_val,
            "request_timestamp": timestamp,
            "success": False,
            "error": str(e)
        }

def calculate_hours_logic(units, sql_hrs):
    return str(sql_hrs)

def get_worker_details_spark():
    try:
        query = """
        SELECT
            HP.INTERNAL_NUM        AS TimeKeeper,
            HP.EMPLOYEE_CODE       AS WorkdayId,
            TT.TOBILL_HRS          AS hrs,
            year(TRAN_DATE)        AS WorkedYear,
            TRAN_DATE              AS timecard_worked_date,
            PERIOD                 AS timecard_worked_cal_period,
            date_format(
                to_date(cast(PERIOD as string), 'yyyyMM'),
                'MMyy'
            )                      AS WP_PPYY,
            POST_DATE              AS Timecard_post_date,
            HC.CLIENT_CODE,
            MATTER_CODE,
            CASE
                WHEN min(POST_DATE) OVER (PARTITION BY HP.INTERNAL_NUM, TRAN_DATE) = POST_DATE
                THEN 'I'
                ELSE 'U'
            END AS InsertUpdate,
            HP.`POSITION`          AS jobtitle
        FROM silver.HBM_MATTER M
        JOIN silver.TAT_TIME TT
            ON M.MATTER_UNO = TT.MATTER_UNO
        JOIN silver.HBM_CLIENT HC
            ON HC.CLIENT_UNO = M.CLIENT_UNO
        JOIN silver.HBM_PERSNL HP
            ON HP.EMPL_UNO = TT.TK_EMPL_UNO
        JOIN silver.HBL_DEPT HD
            ON HD.DEPT_CODE = HP.DEPT
        JOIN silver.HBL_OFFICE HO
            ON HO.OFFC_CODE = HP.OFFC
        WHERE MATTER_CODE IN (
                '8000000028','1000325429','8000000016','1000325434','1000086654'
              )
          AND year(TRAN_DATE) >= year(current_date()) - 1
          AND HP.`POSITION` IN ('Associate', 'Counsel')
          AND HO.OFFC_CODE IN (
                'AUS1','CHI1','DAL1','DEN1','HOU1','LAX1',
                'IPS1','NYC1','PIT1','SAT1','SFO1','STL1','WAS1'
              )
          AND lower(HP.`POSITION`) NOT LIKE '%partner%'
        """
        logger.info("Executing Spark SQL query to get worker details...")
        df = spark.sql(query)
        rows = [row.asDict() for row in df.collect()]
        logger.info(f"Found {len(rows)} records to process.")
        return rows
    except NameError:
        logger.warning("Spark session not available locally. Returning mock data.")
        return [
            {
                "WorkdayId": "50454", 
                "Timecard_post_date": "2026-01-01", 
                "timecard_worked_date": "2026-01-06",
                "hrs": 8.0
            }
        ] 
    except Exception as e:
        logger.error(f"Error executing Spark query: {e}")
        return []

def process_single_row(row, report_endpoint, access_token, dry_run):
    """
    Process a single row, calling GET then POST, returning logs.
    """
    logs = []
    
    workday_id = row.get("WorkdayId")
    prompt_date = row.get("Timecard_post_date")
    worked_date = row.get("timecard_worked_date")
    sql_hrs = row.get("hrs")

    if not all([workday_id, prompt_date, worked_date]):
        return logs

    # 1. Fetch
    entries = fetch_time_off_report_data(
        report_endpoint, access_token, workday_id, prompt_date, worked_date
    )
    
    if not entries and dry_run:
        # For mock testing, simulate finding an entry
        entries = [{
            "timeOffType": {"descriptor": "Vacation Time Off", "id": "vac123"},
            "timeOffEntryWid": "entry123",
            "units": "8"
        }]

    # 2. Process
    for entry in entries:
        time_off_type_obj = entry.get("timeOffType", {})
        time_off_type_val = ""
        if isinstance(time_off_type_obj, dict):
            time_off_type_val = time_off_type_obj.get("descriptor", "")
        else:
            time_off_type_val = str(time_off_type_obj)
        
        if not time_off_type_val and "timeOffType" in entry:
             time_off_type_val = str(entry["timeOffType"])

        if "Vacation" in time_off_type_val:
            wid = entry.get("timeOffEntryWid") or entry.get("WID") or entry.get("id")
            
            if wid:
                units = entry.get("units")
                qty = calculate_hours_logic(units, sql_hrs)
                
                # 3. Post
                result = post_time_off_update(access_token, workday_id, wid, worked_date, qty, dry_run)
                logs.append(result)
    
    return logs

def write_logs_to_table(logs_data, table_name="workday_time_off_logs"):
    """
    Writes the collected logs to a Delta table.
    """
    if not logs_data:
        logger.info("No logs to write.")
        return

    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType
    import json
    
    # Flatten/normalize data for DataFrame
    # Note: Some fields might be missing in some rows, so we should ensure schema consistency
    # We'll dump the whole thing to JSON then let Spark infer or define schema
    
    try:
        rdd = spark.sparkContext.parallelize([json.dumps(r) for r in logs_data])
        df_logs = spark.read.json(rdd)
        
        logger.info(f"Writing {len(logs_data)} log entries to table {table_name}...")
        
        # Save to table (append mode)
        # Using saveAsTable or insertInto depending on environment
        # Here assuming managed table
        df_logs.write.mode("append").saveAsTable(table_name)
        logger.info("Logs successfully written.")
        
    except NameError:
        logger.warning("Spark session not found. Skipping log table write.")
    except Exception as e:
        logger.error(f"Error writing logs: {e}")

def ingest_time_off_process(
    client_id: str, 
    client_secret: str, 
    refresh_token: str, 
    token_endpoint: str,
    report_endpoint: str,
    max_workers: int = 100,
    dry_run: bool = False
):
    logger.info(f"=== Workday Time Off Processing Started (Dry Run: {dry_run}) ===")

    access_token = refresh_workday_access_token(
        client_id, client_secret, refresh_token, token_endpoint
    )

    rows = get_worker_details_spark()
    
    all_logs = []
    
    logger.info(f"Starting threaded processing with {max_workers} threads...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_row = {
            executor.submit(
                process_single_row, 
                row, 
                report_endpoint, 
                access_token,
                dry_run
            ): row 
            for row in rows
        }
        
        completed_count = 0
        total_rows = len(rows)
        log_interval = max(max_workers, 10)

        for future in as_completed(future_to_row):
            completed_count += 1
            try:
                logs = future.result()
                if logs:
                    all_logs.extend(logs)
                
                if completed_count % log_interval == 0 or completed_count == total_rows:
                    logger.info(f"Progress: {completed_count}/{total_rows} records processed.")
                    
            except Exception as exc:
                logger.error(f"Row processing exception: {exc}")

    # Write logs
    write_logs_to_table(all_logs)

    logger.info("=== Workday Time Off Processing Finished ===")

if __name__ == "__main__":
    CLIENT_ID = "your_client_id"
    CLIENT_SECRET = "your_client_secret"
    REFRESH_TOKEN = "your_refresh_token"
    TOKEN_ENDPOINT = "https://wd3-impl-services1.workday.com/ccx/oauth2/nrf3/token"
    TIMEOFF_REPORT_ENDPOINT = "https://wd3-impl-services1.workday.com/ccx/service/customreport2/nrf3/INT0137_USA_HCM_Datahub_Absence_ISU/CRI_INT0137_USA_Datahub_Timeoffs"
    
    # Set dry_run=True for "test first part ingestion"
    ingest_time_off_process(
        CLIENT_ID, 
        CLIENT_SECRET, 
        REFRESH_TOKEN, 
        TOKEN_ENDPOINT, 
        TIMEOFF_REPORT_ENDPOINT,
        max_workers=100,
        dry_run=True 
    )

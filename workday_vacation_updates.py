import requests
import base64
import json
import time
import random
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging with cleaner format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Placeholder for Spark
try:
    spark
except NameError:
    spark = None


# --- LOGGING HELPERS ---
def log_separator(char="-", length=80):
    """Print a visual separator line."""
    logger.info(char * length)


def log_worker_start(workday_id, worked_date):
    """Log the start of processing for a worker."""
    log_separator("=")
    logger.info(f"PROCESSING WORKER: {workday_id} | Date: {worked_date}")
    log_separator("=")


def log_request(method, endpoint, params=None, payload=None):
    """Log HTTP request details in a structured format."""
    logger.info(f"[{method}] {endpoint}")
    if params:
        param_str = ", ".join(f"{k}={v}" for k, v in params.items())
        logger.info(f"  Parameters: {param_str}")
    if payload:
        logger.info(f"  Payload: {json.dumps(payload, indent=4, default=str)}")


def log_response(status_code, entry_count=None, content=None):
    """Log HTTP response details in a structured format."""
    if entry_count is not None:
        logger.info(f"  Response: {status_code} | Entries: {entry_count}")
    else:
        logger.info(f"  Response: {status_code}")
    if content and logger.level <= logging.DEBUG:
        logger.debug(f"  Content: {json.dumps(content, indent=4, default=str)}")


def log_skipped_entries(skipped_list):
    """Log all skipped entries in a consolidated format."""
    if not skipped_list:
        return
    logger.info(f"  Skipped {len(skipped_list)} non-vacation entries:")
    for entry in skipped_list:
        logger.info(f"    - Type: '{entry['type']}' | WID: {entry['wid']}")


def log_vacation_match(wid, quantity):
    """Log when a vacation entry is found and will be processed."""
    logger.info(f"  VACATION MATCH FOUND:")
    logger.info(f"    - WID: {wid}")
    logger.info(f"    - Quantity: {quantity}")


def log_result(success, dry_run=False, error=None):
    """Log the result of a POST operation."""
    if dry_run:
        logger.info(f"  Result: DRY_RUN - No changes made")
    elif success:
        logger.info(f"  Result: SUCCESS")
    else:
        logger.info(f"  Result: FAILED - {error}")


# --- AUTHENTICATION ---
def refresh_workday_access_token(client_id: str, client_secret: str, refresh_token: str, token_endpoint: str) -> str:
    """Refresh the Workday access token using OAuth2."""
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
        logger.info("Access token refreshed successfully")
        return response.json()["access_token"]
    except Exception as e:
        logger.error(f"Failed to refresh access token: {e}")
        raise


# --- DATA FETCHING (SOURCE) ---
def get_worker_details_spark():
    """Fetch worker details from Spark SQL."""
    try:
        query = """
        SELECT
            HP.INTERNAL_NUM        AS TimeKeeper,
            HP.EMPLOYEE_CODE       AS WorkdayId,
            TT.TOBILL_HRS          AS hrs,
            year(TRAN_DATE)        AS WorkedYear,
            TRAN_DATE              AS timecard_worked_date,
            PERIOD                 AS timecard_worked_cal_period,
            date_format(to_date(cast(PERIOD as string), 'yyyyMM'), 'MMyy') AS WP_PPYY,
            POST_DATE              AS Timecard_post_date,
            HC.CLIENT_CODE,
            MATTER_CODE,
            CASE WHEN min(POST_DATE) OVER (PARTITION BY HP.INTERNAL_NUM, TRAN_DATE) = POST_DATE THEN 'I' ELSE 'U' END AS InsertUpdate,
            HP.`POSITION`          AS jobtitle
        FROM silver.HBM_MATTER M
        JOIN silver.TAT_TIME TT ON M.MATTER_UNO = TT.MATTER_UNO
        JOIN silver.HBM_CLIENT HC ON HC.CLIENT_UNO = M.CLIENT_UNO
        JOIN silver.HBM_PERSNL HP ON HP.EMPL_UNO = TT.TK_EMPL_UNO
        JOIN silver.HBL_DEPT HD ON HD.DEPT_CODE = HP.DEPT
        JOIN silver.HBL_OFFICE HO ON HO.OFFC_CODE = HP.OFFC
        WHERE MATTER_CODE IN ('8000000028','1000325429','8000000016','1000325434','1000086654')
          AND year(TRAN_DATE) >= year(current_date()) - 1
          AND HP.`POSITION` IN ('Associate', 'Counsel')
          AND HO.OFFC_CODE IN ('AUS1','CHI1','DAL1','DEN1','HOU1','LAX1','IPS1','NYC1','PIT1','SAT1','SFO1','STL1','WAS1')
          AND lower(HP.`POSITION`) NOT LIKE '%partner%'
        """
        logger.info("Executing Spark SQL query for worker details...")
        df = spark.sql(query)
        rows = [row.asDict() for row in df.collect()]
        logger.info(f"Retrieved {len(rows)} worker records from Spark")
        return rows
    except NameError:
        logger.warning("Spark session not available - using mock data")
        return [{"WorkdayId": "50454", "Timecard_post_date": "2026-01-01", "timecard_worked_date": "2026-01-06", "hrs": 8.0}]
    except Exception as e:
        logger.error(f"Spark query failed: {e}")
        return []


def fetch_time_off_report_data(base_endpoint: str, access_token: str, colleague_id: str, prompt_date: str, work_date: str):
    """Fetch time off report data from Workday API."""
    try:
        def fmt_date(d):
            return d.strftime("%Y-%m-%d") if isinstance(d, datetime) else str(d).split('T')[0]

        formatted_prompt = f"{fmt_date(prompt_date)}-08:00"
        formatted_work = f"{fmt_date(work_date)}-08:00"
        
        params = {
            "promptDate1": formatted_prompt,
            "Include_Terminated_Workers": "0",
            "Colleague_ID": colleague_id,
            "date": formatted_work,
            "format": "json"
        }
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        
        # Log the request
        log_request("GET", base_endpoint, params=params)
        
        response = requests.get(base_endpoint, headers=headers, params=params, timeout=30)
        entries = response.json().get("Report_Entry", []) if response.status_code == 200 else []
        
        # Log the response
        log_response(response.status_code, entry_count=len(entries), content=entries)
        
        return entries
    except Exception as e:
        logger.error(f"  Request failed: {e}")
        return []


# --- PAYLOAD BUILDERS ---
def build_vacation_payload(time_off_entry_wid: str, date_val: str, quantity: str):
    """Construct the payload for vacation time off request."""
    formatted_date = date_val if "T" in str(date_val) else f"{date_val}T08:00:00.000Z"
    return {
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


# --- RESPONSE PARSERS ---
def parse_time_off_response(response_json):
    """Parse the Workday time off POST response."""
    try:
        days_list = response_json.get("days", [])
        day_entry = days_list[0] if days_list else {}
        bp_params = response_json.get("businessProcessParameters", {})
        
        return {
            "id": day_entry.get("id"),
            "descriptor": day_entry.get("descriptor"),
            "comment": day_entry.get("comment"),
            "timeOffType_id": day_entry.get("timeOffType", {}).get("id"),
            "timeOffType_descriptor": day_entry.get("timeOffType", {}).get("descriptor"),
            "date": day_entry.get("date"),
            "dailyQuantity": day_entry.get("dailyQuantity"),
            "transactionStatus": bp_params.get("transactionStatus", {}).get("descriptor")
        }
    except Exception as e:
        logger.error(f"Failed to parse response: {e}")
        return {"parse_error": str(e)}


# --- GENERIC API EXECUTOR ---
def execute_post_request(url: str, payload: dict, access_token: str, dry_run: bool = False):
    """Execute a POST request to the Workday API."""
    timestamp = datetime.utcnow().isoformat()
    
    if dry_run:
        return {
            "status": "DRY_RUN",
            "timestamp": timestamp,
            "url": url,
            "payload": payload,
            "success": True
        }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        
        result = {
            "timestamp": timestamp,
            "http_status": response.status_code,
            "response_text": response.text,
            "success": response.status_code in (200, 201)
        }
        
        if result["success"]:
            try:
                result["json_data"] = response.json()
            except:
                result["json_data"] = {}
                
        return result

    except Exception as e:
        return {
            "timestamp": timestamp,
            "success": False,
            "error": str(e)
        }


# --- ENTRY EXTRACTION HELPERS ---
def extract_time_off_type(entry):
    """Extract time off type descriptor from various response formats."""
    type_obj = entry.get("timeOffType") or entry.get("Time_Off_Type") or entry.get("TimeOffType")
    
    if isinstance(type_obj, dict):
        return type_obj.get("descriptor", "") or type_obj.get("@Descriptor", "")
    elif isinstance(type_obj, str):
        return type_obj
    
    return entry.get("timeOffType_descriptor") or entry.get("Time_Off_Type_descriptor") or ""


def extract_wid(entry):
    """Extract WID from various response formats."""
    wid = entry.get("timeOffEntryWid") or entry.get("Time_Off_Entry_WID") or entry.get("id") or entry.get("WID")
    
    if isinstance(wid, dict): 
        return wid.get("id") or wid.get("#text")
    
    return wid


def flatten_time_off_entries(parent_entry):
    """Flatten nested time off entry structures into a list."""
    items = []
    
    if "Time_Off_Completed_Details_group" in parent_entry:
        group = parent_entry["Time_Off_Completed_Details_group"]
        if isinstance(group, list):
            items.extend(group)
        elif isinstance(group, dict):
            items.append(group)
    else:
        items.append(parent_entry)
    
    return items


# --- BUSINESS LOGIC ---
def calculate_hours_logic(units, sql_hrs):
    """Calculate the hours to submit."""
    return str(sql_hrs)


def process_single_row(row, report_endpoint, access_token, dry_run):
    """Process a single worker row and return log entries."""
    logs = []
    workday_id = row.get("WorkdayId")
    prompt_date = row.get("Timecard_post_date")
    worked_date = row.get("timecard_worked_date")
    sql_hrs = row.get("hrs")

    if not all([workday_id, prompt_date, worked_date]):
        logger.warning(f"Skipping row - missing required fields: WorkdayId={workday_id}, prompt_date={prompt_date}, worked_date={worked_date}")
        return logs

    # Log worker processing start
    log_worker_start(workday_id, worked_date)
    
    # Fetch existing time off entries
    entries = fetch_time_off_report_data(report_endpoint, access_token, workday_id, prompt_date, worked_date)

    if not entries and dry_run:
        entries = [{"timeOffType": {"descriptor": "Vacation"}, "timeOffEntryWid": "mock_wid", "units": "8"}]

    # Process entries and collect skipped items
    skipped_entries = []
    vacation_entries = []
    
    for parent_entry in entries:
        items = flatten_time_off_entries(parent_entry)
        
        for entry in items:
            type_desc = extract_time_off_type(entry)
            wid = extract_wid(entry)
            
            if "Vacation" in type_desc:
                vacation_entries.append((entry, wid))
            elif type_desc:
                skipped_entries.append({"type": type_desc, "wid": wid or "N/A"})
    
    # Log all skipped entries together
    log_skipped_entries(skipped_entries)
    
    # Process vacation entries
    for entry, wid in vacation_entries:
        if not wid:
            logger.warning("  Vacation entry found but WID is missing - skipping")
            continue
            
        qty = calculate_hours_logic(entry.get("units"), sql_hrs)
        log_vacation_match(wid, qty)
        
        # Build payload and URL
        payload = build_vacation_payload(wid, worked_date, qty)
        url = f"https://wd3-impl-services1.workday.com/ccx/api/absenceManagement/v3/nrf3/workers/{workday_id}/requestTimeOff"
        
        # Log the POST request
        log_request("POST", url, payload=payload)
        
        # Execute the request
        result = execute_post_request(url, payload, access_token, dry_run)
        
        # Log the result
        log_result(result["success"], dry_run, result.get("error") or result.get("response_text"))
        
        # Build log entry
        log_entry = {
            "worker_id": workday_id,
            "request_date": str(worked_date),
            "success": result["success"],
            "timestamp": result["timestamp"],
            "time_off_type": "Vacation",
            "wid": wid,
            "quantity": qty,
            "dry_run": dry_run
        }
        
        if result["success"] and not dry_run:
            parsed_fields = parse_time_off_response(result.get("json_data", {}))
            log_entry.update(parsed_fields)
        elif dry_run:
            log_entry["status"] = "DRY_RUN"
        else:
            log_entry["error"] = result.get("error") or result.get("response_text")
        
        logs.append(log_entry)
    
    # Summary for this worker
    if vacation_entries:
        logger.info(f"  Worker {workday_id} summary: {len(vacation_entries)} vacation entries processed, {len(skipped_entries)} skipped")
    else:
        logger.info(f"  Worker {workday_id} summary: No vacation entries found, {len(skipped_entries)} skipped")
    
    return logs


# --- LOGGING UTILS ---
def write_logs_to_table(logs_data, table_name="workday_time_off_logs"):
    """Write processing logs to a Spark table."""
    if not logs_data:
        logger.info("No logs to write")
        return
    
    def json_serial(obj):
        if isinstance(obj, (datetime, datetime.date)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    try:
        rdd = spark.sparkContext.parallelize([json.dumps(r, default=json_serial) for r in logs_data])
        df_logs = spark.read.json(rdd)
        df_logs.write.mode("append").saveAsTable(table_name)
        logger.info(f"Written {len(logs_data)} logs to {table_name}")
    except NameError:
        logger.warning("Spark unavailable - logs not persisted to table")
    except Exception as e:
        logger.error(f"Failed to write logs to table: {e}")


# --- MAIN EXECUTOR ---
def ingest_time_off_process(client_id, client_secret, refresh_token, token_url, report_url, max_workers=100, dry_run=False):
    """Main entry point for the time off ingestion process."""
    log_separator("*")
    logger.info(f"STARTING TIME OFF INGESTION PROCESS")
    logger.info(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info(f"  Max Workers: {max_workers}")
    log_separator("*")
    
    # Get access token
    token = refresh_workday_access_token(client_id, client_secret, refresh_token, token_url)
    
    # Fetch worker data
    rows = get_worker_details_spark()
    logger.info(f"Processing {len(rows)} worker records...")
    
    all_logs = []
    success_count = 0
    error_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_row, row, report_url, token, dry_run): row for row in rows}
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            
            # Progress logging every 10% or every max_workers
            progress_interval = max(len(rows) // 10, max_workers, 1)
            if completed % progress_interval == 0 or completed == len(rows):
                logger.info(f"Progress: {completed}/{len(rows)} ({100*completed//len(rows)}%)")
            
            try:
                logs = future.result()
                if logs:
                    all_logs.extend(logs)
                    success_count += sum(1 for l in logs if l.get("success"))
                    error_count += sum(1 for l in logs if not l.get("success"))
            except Exception as e:
                logger.error(f"Worker thread error: {e}")
                error_count += 1

    # Write logs to table
    write_logs_to_table(all_logs)
    
    # Final summary
    log_separator("*")
    logger.info("PROCESS COMPLETE")
    logger.info(f"  Total Records Processed: {len(rows)}")
    logger.info(f"  Vacation Requests Made: {len(all_logs)}")
    logger.info(f"  Successful: {success_count}")
    logger.info(f"  Failed: {error_count}")
    log_separator("*")


if __name__ == "__main__":
    # Example usage - replace with actual credentials
    CLIENT_ID = "your_client_id"
    CLIENT_SECRET = "your_client_secret"
    REFRESH_TOKEN = "your_refresh_token"
    TOKEN_URL = "https://wd3-impl-services1.workday.com/ccx/oauth2/nrf3/token"
    REPORT_URL = "https://wd3-impl-services1.workday.com/ccx/service/customreport2/nrf3/INT0137_USA_HCM_Datahub_Absence_ISU/CRI_INT0137_USA_Datahub_Timeoffs"
    
    ingest_time_off_process(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        refresh_token=REFRESH_TOKEN,
        token_url=TOKEN_URL,
        report_url=REPORT_URL,
        max_workers=10,
        dry_run=True
    )

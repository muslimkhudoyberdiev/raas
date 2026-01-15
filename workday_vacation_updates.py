import requests
import base64
import json
import time
import random
import logging
from datetime import datetime, timedelta, date
from decimal import Decimal
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


def log_response(status_code, content=None):
    """Log HTTP response details in a structured format."""
    logger.info(f"  Response Status: {status_code}")
    if content:
        logger.info(f"  Response Body:\n{json.dumps(content, indent=4, default=str)}")


def log_skipped_entries(skipped_list):
    """Log all skipped entries in a consolidated format."""
    if not skipped_list:
        return
    logger.info(f"  Skipped {len(skipped_list)} unsupported time off entries:")
    for entry in skipped_list:
        logger.info(f"    - Type: '{entry['type']}' | WID: {entry['wid']}")


def log_time_off_match(time_off_type, wid, quantity, unit_of_time="Hours"):
    """Log when a time off entry is found and will be processed."""
    logger.info(f"  TIME OFF MATCH FOUND:")
    logger.info(f"    - Type: {time_off_type}")
    logger.info(f"    - WID: {wid}")
    logger.info(f"    - Quantity: {quantity} {unit_of_time}")


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
        log_response(response.status_code, content=entries)
        
        return entries
    except Exception as e:
        logger.error(f"  Request failed: {e}")
        return []


# --- SUPPORTED TIME OFF TYPES ---
SUPPORTED_TIME_OFF_TYPES = {
    "Vacation": {
        "descriptor": "Vacation Time Off",
        "unit_of_time": "Hours"
    },
    "Lawyer Supplement Time Off": {
        "descriptor": "Lawyer Supplement Time Off",
        "unit_of_time": "Days"
    }
}


def get_supported_type_key(type_desc: str) -> str:
    """Check if time off type is supported and return the key."""
    for key in SUPPORTED_TIME_OFF_TYPES:
        if key in type_desc:
            return key
    return None


# --- PAYLOAD BUILDERS ---
def build_time_off_payload(time_off_entry_wid: str, date_val: str, quantity: str, time_off_type_key: str):
    """Construct the payload for ENTER time off request based on type."""
    formatted_date = date_val if "T" in str(date_val) else f"{date_val}T08:00:00.000Z"
    
    type_config = SUPPORTED_TIME_OFF_TYPES.get(time_off_type_key, SUPPORTED_TIME_OFF_TYPES["Vacation"])
    
    return {
        "days": [
            {
                "dailyQuantity": str(quantity),
                "comment": "INT0137",
                "timeOffType": {
                    "descriptor": type_config["descriptor"],
                    "id": time_off_entry_wid 
                },
                "date": formatted_date
            }
        ]
    }


def build_adjust_time_off_payload(time_off_entry_wid: str, date_val: str, quantity: str = "0"):
    """
    Construct the payload for ADJUST time off request (used to zero out existing entries).
    
    Note: To update an entry, you must first adjust it to zero, then enter the new value.
    """
    formatted_date = date_val if "T" in str(date_val) else f"{date_val}T08:00:00.000Z"
    
    return {
        "days": [
            {
                "dailyQuantity": str(quantity),
                "comment": "INT0137 - Adjustment",
                "timeOffEntry": {
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
def calculate_quantity(time_off_type_key: str, entry_units: str, sql_hrs: float, insert_update: str = "I") -> dict:
    """
    Calculate the quantity to submit based on time off type and insert/update flag.
    
    Logic:
    - INSERT (I): Use sql_hrs directly (new entry)
    - UPDATE (U): sql_hrs is NEGATIVE (reduction amount)
      - Need to zero out first, then re-enter with new value
      - New value = existing units + sql_hrs (since sql_hrs is negative)
    
    For Lawyer Supplement Time Off:
    - Uses Days as unit (not Hours)
    - sql_hrs is already in days from the source
    
    Args:
        time_off_type_key: The type of time off (e.g., "Vacation", "Lawyer Supplement Time Off")
        entry_units: The existing units value from the API response entry
        sql_hrs: Hours/Days from the SQL query (negative for updates)
        insert_update: "I" for insert, "U" for update
        
    Returns:
        Dictionary with:
        - quantity: The quantity to enter
        - needs_zero_out: Whether we need to zero out first (for updates)
        - is_removal: Whether this is a complete removal (quantity = 0)
    """
    result = {
        "quantity": "0",
        "needs_zero_out": False,
        "is_removal": False
    }
    
    # Parse existing units from API response
    existing_units = 0.0
    if entry_units:
        try:
            existing_units = float(entry_units)
        except (ValueError, TypeError):
            existing_units = 0.0
    
    # Parse sql_hrs
    hrs_value = 0.0
    if sql_hrs is not None:
        try:
            hrs_value = float(sql_hrs)
        except (ValueError, TypeError):
            hrs_value = 0.0
    
    if insert_update == "U":
        # UPDATE case: sql_hrs is negative (reduction amount)
        # Need to zero out first, then re-enter with new value
        result["needs_zero_out"] = True
        
        # Calculate new quantity: existing + adjustment (adjustment is negative)
        new_quantity = existing_units + hrs_value
        
        if new_quantity <= 0:
            # Complete removal - just zero out
            result["quantity"] = "0"
            result["is_removal"] = True
        else:
            result["quantity"] = str(round(new_quantity, 2))
    else:
        # INSERT case: sql_hrs is the actual quantity to enter
        if time_off_type_key == "Lawyer Supplement Time Off":
            # Lawyer Supplement: sql_hrs is already in days
            result["quantity"] = str(round(abs(hrs_value), 2)) if hrs_value else str(existing_units) if existing_units else "1"
        else:
            # Vacation: sql_hrs is in hours
            result["quantity"] = str(round(abs(hrs_value), 2)) if hrs_value else str(existing_units) if existing_units else "0"
    
    return result


def log_calculation_details(insert_update: str, existing_units: str, sql_hrs: float, calc_result: dict):
    """Log the calculation details for debugging."""
    logger.info(f"  Calculation Details:")
    logger.info(f"    - Operation: {'UPDATE' if insert_update == 'U' else 'INSERT'}")
    logger.info(f"    - Existing Units: {existing_units}")
    logger.info(f"    - SQL Hours/Days: {sql_hrs}")
    logger.info(f"    - Calculated Quantity: {calc_result['quantity']}")
    if calc_result['needs_zero_out']:
        logger.info(f"    - Action: Zero out first, then {'remove completely' if calc_result['is_removal'] else 're-enter with ' + calc_result['quantity']}")


def execute_adjust_time_off(workday_id: str, wid: str, date_val: str, access_token: str, dry_run: bool = False):
    """
    Execute the adjust time off API to zero out an existing entry.
    This is required before re-entering a new value for updates.
    """
    payload = build_adjust_time_off_payload(wid, date_val, "0")
    url = f"https://wd3-impl-services1.workday.com/ccx/api/absenceManagement/v3/nrf3/workers/{workday_id}/adjustTimeOff"
    
    logger.info(f"  [ZERO OUT] Adjusting existing entry to 0")
    log_request("POST", url, payload=payload)
    
    result = execute_post_request(url, payload, access_token, dry_run)
    log_result(result["success"], dry_run, result.get("error") or result.get("response_text"))
    
    return result


def process_single_row(row, report_endpoint, access_token, dry_run):
    """Process a single worker row and return log entries."""
    logs = []
    workday_id = row.get("WorkdayId")
    prompt_date = row.get("Timecard_post_date")
    worked_date = row.get("timecard_worked_date")
    sql_hrs = row.get("hrs")
    insert_update = row.get("InsertUpdate", "I")  # Default to Insert if not specified

    if not all([workday_id, prompt_date, worked_date]):
        logger.warning(f"Skipping row - missing required fields: WorkdayId={workday_id}, prompt_date={prompt_date}, worked_date={worked_date}")
        return logs

    # Log worker processing start
    log_worker_start(workday_id, worked_date)
    logger.info(f"  Operation: {'UPDATE' if insert_update == 'U' else 'INSERT'} | SQL Hours: {sql_hrs}")
    
    # Fetch existing time off entries from RAS report
    entries = fetch_time_off_report_data(report_endpoint, access_token, workday_id, prompt_date, worked_date)

    if not entries and dry_run:
        # Mock entries for testing both supported types
        entries = [
            {"timeOffType": {"descriptor": "Vacation"}, "timeOffEntryWid": "mock_vacation_wid", "units": "8"},
            {"timeOffType": {"descriptor": "Lawyer Supplement Time Off"}, "timeOffEntryWid": "mock_lawyer_wid", "units": "1", "unitOfTime": "Days"}
        ]

    # Process entries and collect supported/skipped items
    skipped_entries = []
    supported_entries = []  # List of (entry, wid, type_key, type_desc)
    
    for parent_entry in entries:
        items = flatten_time_off_entries(parent_entry)
        
        for entry in items:
            type_desc = extract_time_off_type(entry)
            wid = extract_wid(entry)
            
            # Check if this is a supported time off type
            type_key = get_supported_type_key(type_desc)
            if type_key:
                supported_entries.append((entry, wid, type_key, type_desc))
            elif type_desc:
                skipped_entries.append({"type": type_desc, "wid": wid or "N/A"})
    
    # Log all skipped entries together
    log_skipped_entries(skipped_entries)
    
    # Process supported time off entries
    for entry, wid, type_key, type_desc in supported_entries:
        if not wid:
            logger.warning(f"  {type_key} entry found but WID is missing - skipping")
            continue
        
        # Get type configuration
        type_config = SUPPORTED_TIME_OFF_TYPES[type_key]
        unit_of_time = type_config["unit_of_time"]
        
        # Get existing units from entry
        existing_units = entry.get("units") or entry.get("Total_Units") or "0"
        
        # Calculate quantity based on type and insert/update flag
        calc_result = calculate_quantity(type_key, existing_units, sql_hrs, insert_update)
        
        # Log calculation details
        log_calculation_details(insert_update, existing_units, sql_hrs, calc_result)
        log_time_off_match(type_key, wid, calc_result["quantity"], unit_of_time)
        
        # Initialize log entry
        log_entry = {
            "worker_id": workday_id,
            "request_date": str(worked_date),
            "time_off_type": type_key,
            "wid": wid,
            "operation": "UPDATE" if insert_update == "U" else "INSERT",
            "existing_units": existing_units,
            "sql_hrs": sql_hrs,
            "calculated_quantity": calc_result["quantity"],
            "unit_of_time": unit_of_time,
            "dry_run": dry_run,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Handle UPDATE case: need to zero out first
        if calc_result["needs_zero_out"]:
            logger.info(f"  UPDATE detected - zeroing out existing entry first")
            
            # Step 1: Zero out the existing entry
            zero_result = execute_adjust_time_off(workday_id, wid, worked_date, access_token, dry_run)
            log_entry["zero_out_success"] = zero_result["success"]
            
            if not zero_result["success"]:
                log_entry["success"] = False
                log_entry["error"] = f"Failed to zero out: {zero_result.get('error') or zero_result.get('response_text')}"
                logs.append(log_entry)
                continue
            
            # Step 2: If this is a complete removal, we're done
            if calc_result["is_removal"]:
                logger.info(f"  Complete removal - entry zeroed out, no re-entry needed")
                log_entry["success"] = True
                log_entry["action"] = "REMOVED"
                logs.append(log_entry)
                continue
            
            # Step 3: Re-enter with new value
            logger.info(f"  Re-entering with new value: {calc_result['quantity']}")
        
        # Build payload and URL for enter time off
        payload = build_time_off_payload(wid, worked_date, calc_result["quantity"], type_key)
        url = f"https://wd3-impl-services1.workday.com/ccx/api/absenceManagement/v3/nrf3/workers/{workday_id}/requestTimeOff"
        
        # Log the POST request
        log_request("POST", url, payload=payload)
        
        # Execute the request
        result = execute_post_request(url, payload, access_token, dry_run)
        
        # Log the result
        log_result(result["success"], dry_run, result.get("error") or result.get("response_text"))
        
        # Update log entry with result
        log_entry["success"] = result["success"]
        log_entry["action"] = "UPDATED" if insert_update == "U" else "INSERTED"
        
        if result["success"] and not dry_run:
            parsed_fields = parse_time_off_response(result.get("json_data", {}))
            log_entry.update(parsed_fields)
        elif dry_run:
            log_entry["status"] = "DRY_RUN"
        else:
            log_entry["error"] = result.get("error") or result.get("response_text")
        
        logs.append(log_entry)
    
    # Summary for this worker
    if supported_entries:
        type_counts = {}
        for _, _, type_key, _ in supported_entries:
            type_counts[type_key] = type_counts.get(type_key, 0) + 1
        type_summary = ", ".join(f"{k}: {v}" for k, v in type_counts.items())
        logger.info(f"  Worker {workday_id} summary: {len(supported_entries)} entries processed ({type_summary}), {len(skipped_entries)} skipped")
    else:
        logger.info(f"  Worker {workday_id} summary: No supported time off entries found, {len(skipped_entries)} skipped")
    
    return logs


# --- LOGGING UTILS ---
def sanitize_value(value):
    """Sanitize a single value to ensure it's JSON-serializable."""
    if value is None:
        return None
    elif isinstance(value, (datetime, date)):
        return value.isoformat()
    elif isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, (str, int, float, bool)):
        return value
    elif isinstance(value, dict):
        return sanitize_log_entry(value)
    elif isinstance(value, (list, tuple)):
        return [sanitize_value(v) for v in value]
    else:
        # Convert anything else to string
        return str(value)


def sanitize_log_entry(entry):
    """
    Sanitize a log entry to ensure all values are JSON-serializable.
    Converts non-serializable types to appropriate JSON types.
    """
    if not isinstance(entry, dict):
        return sanitize_value(entry)
    
    sanitized = {}
    for key, value in entry.items():
        sanitized[key] = sanitize_value(value)
    return sanitized


def write_logs_to_table(logs_data, table_name="workday_time_off_logs"):
    """Write processing logs to a Spark table."""
    if not logs_data:
        logger.info("No logs to write")
        return
    
    try:
        # Sanitize all log entries first
        sanitized_logs = [sanitize_log_entry(entry) for entry in logs_data]
        
        # Log sample of what we're writing
        logger.info(f"Writing {len(sanitized_logs)} log entries to {table_name}")
        if sanitized_logs:
            logger.info(f"Sample log entry keys: {list(sanitized_logs[0].keys())}")
        
        # Convert to JSON strings
        json_strings = [json.dumps(entry) for entry in sanitized_logs]
        
        # Write to Spark table
        rdd = spark.sparkContext.parallelize(json_strings)
        df_logs = spark.read.json(rdd)
        df_logs.write.mode("append").saveAsTable(table_name)
        logger.info(f"Successfully written {len(sanitized_logs)} logs to {table_name}")
        
    except NameError:
        logger.warning("Spark unavailable - logs not persisted to table")
        # Print logs to console as fallback
        logger.info("Logs that would have been written:")
        for entry in logs_data[:3]:  # Show first 3 entries
            logger.info(f"  {entry}")
        if len(logs_data) > 3:
            logger.info(f"  ... and {len(logs_data) - 3} more entries")
    except Exception as e:
        logger.error(f"Failed to write logs to table: {e}")
        # Log the problematic data for debugging
        logger.error(f"Error details: {type(e).__name__}: {e}")
        if logs_data:
            try:
                sample = sanitize_log_entry(logs_data[0])
                logger.error(f"Sample sanitized entry: {json.dumps(sample, indent=2)}")
            except Exception as inner_e:
                logger.error(f"Could not serialize sample entry: {inner_e}")


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
    logger.info(f"  Time Off Requests Made: {len(all_logs)}")
    logger.info(f"  Successful: {success_count}")
    logger.info(f"  Failed: {error_count}")
    
    # Breakdown by type
    if all_logs:
        type_breakdown = {}
        for log in all_logs:
            t = log.get("time_off_type", "Unknown")
            type_breakdown[t] = type_breakdown.get(t, 0) + 1
        logger.info(f"  Breakdown by Type:")
        for t, count in type_breakdown.items():
            logger.info(f"    - {t}: {count}")
    
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

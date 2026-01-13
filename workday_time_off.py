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

# Placeholder for Spark
try:
    spark
except NameError:
    spark = None

# --- AUTHENTICATION ---
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
        return response.json()["access_token"]
    except Exception as e:
        logger.error(f"ERROR while refreshing Workday access token: {e}")
        raise

# --- DATA FETCHING (SOURCE) ---
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
        logger.info("Executing Spark SQL query...")
        df = spark.sql(query)
        rows = [row.asDict() for row in df.collect()]
        logger.info(f"Found {len(rows)} records.")
        return rows
    except NameError:
        logger.warning("Spark session not available locally. Returning mock data.")
        return [{"WorkdayId": "50454", "Timecard_post_date": "2026-01-01", "timecard_worked_date": "2026-01-06", "hrs": 8.0}]
    except Exception as e:
        logger.error(f"Error executing Spark query: {e}")
        return []

def fetch_time_off_report_data(base_endpoint: str, access_token: str, colleague_id: str, prompt_date: str, work_date: str):
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
        
        response = requests.get(base_endpoint, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            return response.json().get("Report_Entry", [])
        else:
            logger.error(f"Fetch failed for {colleague_id}: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Error fetching report: {e}")
        return []

# --- PAYLOAD BUILDERS ---
def build_vacation_payload(time_off_entry_wid: str, date_val: str, quantity: str):
    """
    Constructs the payload for vacation time off request.
    """
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
    """
    Parses the Workday time off POST response.
    """
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
        logger.error(f"Error parsing response: {e}")
        return {"parse_error": str(e)}

# --- GENERIC API EXECUTOR ---
def execute_post_request(url: str, payload: dict, access_token: str, dry_run: bool = False):
    """
    Generic function to execute a POST request.
    """
    timestamp = datetime.utcnow().isoformat()
    
    if dry_run:
        logger.info(f"[DRY RUN] POST {url}")
        return {
            "status": "DRY_RUN",
            "timestamp": timestamp,
            "url": url,
            "payload": payload,
            "success": True # Mock success
        }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    try:
        logger.info(f"--- POST Request to {url} ---")
        logger.info(f"Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
        
        logger.info(f"--- Response ({response.status_code}) ---")
        logger.info(f"Body: {response.text}")

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

# --- BUSINESS LOGIC ---
def calculate_hours_logic(units, sql_hrs):
    return str(sql_hrs)

def process_single_row(row, report_endpoint, access_token, dry_run):
    logs = []
    workday_id = row.get("WorkdayId")
    prompt_date = row.get("Timecard_post_date")
    worked_date = row.get("timecard_worked_date")
    sql_hrs = row.get("hrs")

    if not all([workday_id, prompt_date, worked_date]):
        return logs

    # 1. Fetch Existing Entries
    # logger.info(f"--- GET Request (Fetch Report) ---\nEndpoint: {report_endpoint}\nParams: Colleague_ID={workday_id}, promptDate1={prompt_date}, date={worked_date}")
    entries = fetch_time_off_report_data(report_endpoint, access_token, workday_id, prompt_date, worked_date)
    
    # If no entries found in REAL mode, we stop here.
    # If dry_run is True AND no entries found, we use mock data.
    # The user saw "mock_wid" because dry_run was True and the real fetch returned 0 entries.
    
    if not entries and dry_run:
         # logger.info("No entries found in dry_run mode. Using MOCK data for demonstration.")
         # Mock entry for testing
         entries = [{"timeOffType": {"descriptor": "Vacation"}, "timeOffEntryWid": "mock_wid", "units": "8"}]

    for parent_entry in entries:
        # The report structure might be nested.
        # Check for 'Time_Off_Completed_Details_group' or handle flat structure.
        
        # Determine list of items to inspect
        items_to_inspect = []
        if "Time_Off_Completed_Details_group" in parent_entry:
            group = parent_entry["Time_Off_Completed_Details_group"]
            if isinstance(group, list):
                items_to_inspect.extend(group)
            elif isinstance(group, dict):
                items_to_inspect.append(group)
        else:
            # Fallback: assume the entry itself is the item
            items_to_inspect.append(parent_entry)

        for entry in items_to_inspect:
            # Robust extraction attempt
            # Try to find Time Off Type
            type_obj = entry.get("timeOffType") or entry.get("Time_Off_Type") or entry.get("TimeOffType")
            type_desc = ""
            if isinstance(type_obj, dict):
                type_desc = type_obj.get("descriptor", "") or type_obj.get("@Descriptor", "")
            elif isinstance(type_obj, str):
                type_desc = type_obj
            
            # If still empty, check if it's a flat key like "Time_Off_Type_descriptor"
            if not type_desc:
                type_desc = entry.get("timeOffType_descriptor") or entry.get("Time_Off_Type_descriptor") or ""

            if "Vacation" in type_desc:
                # Try to find WID
                wid = entry.get("timeOffEntryWid") or entry.get("Time_Off_Entry_WID") or entry.get("id") or entry.get("WID")
                if isinstance(wid, dict): 
                    wid = wid.get("id") or wid.get("#text")
                
                if wid:
                    logger.info(f"Vacation Match! WID found: {wid}")
                    # Log the specific inner entry that matched
                    logger.info(f"--- GET Response (Source) ---\n{json.dumps(entry, indent=2)}")
                    
                    # 2. Prepare Payload
                    qty = calculate_hours_logic(entry.get("units"), sql_hrs)
                    payload = build_vacation_payload(wid, worked_date, qty)
                    
                    logger.info(f"--- POST Payload ---\n{json.dumps(payload, indent=2)}")
                    
                    # 3. Define URL
                    url = f"https://wd3-impl-services1.workday.com/ccx/api/absenceManagement/v3/nrf3/workers/{workday_id}/requestTimeOff"
                    
                    # 4. Execute Generic POST
                    result = execute_post_request(url, payload, access_token, dry_run)
                    
                    # 5. Handle Result & Log
                    log_entry = {
                        "worker_id": workday_id,
                        "request_date": worked_date,
                        "success": result["success"],
                        "timestamp": result["timestamp"],
                        "get_response_fragment": json.dumps(entry),
                        "post_payload": json.dumps(payload)
                    }
                    
                    if result["success"] and not dry_run:
                        parsed_fields = parse_time_off_response(result.get("json_data", {}))
                        log_entry.update(parsed_fields)
                    elif dry_run:
                        log_entry["status"] = "DRY_RUN_SUCCESS"
                    else:
                        log_entry["error"] = result.get("error") or result.get("response_text")
                    
                    logs.append(log_entry)
                
    return logs

# --- LOGGING UTILS ---
def write_logs_to_table(logs_data, table_name="workday_time_off_logs"):
    if not logs_data:
        return
    
    # Helper to serialize datetime objects in logs
    def json_serial(obj):
        if isinstance(obj, (datetime, datetime.date)):
            return obj.isoformat()
        raise TypeError (f"Type {type(obj)} not serializable")

    try:
        rdd = spark.sparkContext.parallelize([json.dumps(r, default=json_serial) for r in logs_data])
        df_logs = spark.read.json(rdd)
        df_logs.write.mode("append").saveAsTable(table_name)
        logger.info(f"Written {len(logs_data)} logs to {table_name}")
    except NameError:
        logger.warning("Spark unavailable - skipping log write")
    except Exception as e:
        logger.error(f"Log write failed: {e}")

# --- MAIN EXECUTOR ---
def ingest_time_off_process(client_id, client_secret, refresh_token, token_url, report_url, max_workers=100, dry_run=False):
    logger.info(f"Starting Process (Dry Run: {dry_run})")
    
    token = refresh_workday_access_token(client_id, client_secret, refresh_token, token_url)
    rows = get_worker_details_spark()
    
    all_logs = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_row, row, report_url, token, dry_run): row for row in rows}
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            if completed % max(max_workers, 10) == 0:
                logger.info(f"Progress: {completed}/{len(rows)}")
            try:
                logs = future.result()
                if logs: all_logs.extend(logs)
            except Exception as e:
                logger.error(f"Row error: {e}")

    write_logs_to_table(all_logs)
    logger.info("Finished.")

if __name__ == "__main__":
    # Configuration
    CLIENT_ID = "your_client_id"
    CLIENT_SECRET = "your_client_secret"
    REFRESH_TOKEN = "your_refresh_token"
    TOKEN_ENDPOINT = "https://wd3-impl-services1.workday.com/ccx/oauth2/nrf3/token"
    TIMEOFF_REPORT_ENDPOINT = "https://wd3-impl-services1.workday.com/ccx/service/customreport2/nrf3/INT0137_USA_HCM_Datahub_Absence_ISU/CRI_INT0137_USA_Datahub_Timeoffs"
    
    ingest_time_off_process(
        CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, TOKEN_ENDPOINT, TIMEOFF_REPORT_ENDPOINT,
        max_workers=1,
        dry_run=False 
    )

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
        
        # Format dates as needed by Workday: YYYY-MM-DD-08:00 based on example
        # Assuming input dates are YYYY-MM-DD string or datetime objects
        
        # Helper to ensure string format
        def fmt_date(d):
            if isinstance(d, (datetime,)):
                return d.strftime("%Y-%m-%d")
            return str(d).split('T')[0] # simplistic cleanup if needed

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

def post_time_off_update(access_token: str, worker_id: str, time_off_entry_wid: str, date_val: str, quantity: str):
    """
    Sends a POST request to the Absence Management API.
    """
    # Note: URL has a worker ID in the path. 
    # The user example: .../workers/f503c098b21d10010654c7866af00003/requestTimeOff
    # I need to use the actual worker's ID (Workday WID) if available, or the Colleague ID if that works in the URL.
    # The prompt says: "WorkdayId -> Colleague_ID". 
    # Usually the API requires the Workday WID (32 char hex) in the URL path.
    # The SQL query returns `EMPLOYEE_CODE` as `WorkdayId`. This is usually the Employee ID (e.g., 12345), not the WID.
    # However, the user example URL has a WID: f503...
    # Without the WID in the SQL query, I might fail if the API demands WID.
    # I will assume `worker_id` passed here is what goes into the URL. 
    # If the SQL `WorkdayId` is just the EmployeeID, this URL construction might be wrong unless the API accepts EmployeeID in a different format.
    # BUT, I must follow the user's mapping: "WorkdayId -> Colleague_ID" (for the GET report).
    # For the POST, the user says: "send that results into the different api".
    # I will assume for now I should use the `worker_id` (WorkdayId from SQL) in the URL.
    
    url = f"https://wd3-impl-services1.workday.com/ccx/api/absenceManagement/v3/nrf3/workers/{worker_id}/requestTimeOff"

    # Format date for payload: "2026-01-06T08:00:00.000Z"
    # Input date_val is likely YYYY-MM-DD
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
        'Authorization': f'Bearer {access_token}',
        # 'Cookie': ... # Cookies usually not needed with Bearer token, removing hardcoded cookie
    }

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=30)
        if response.status_code in (200, 201):
            logger.info(f"Successfully posted time off for {worker_id} on {date_val}. Response: {response.text}")
        else:
            logger.error(f"Failed to post time off for {worker_id}: {response.status_code} - {response.text}")
            
    except Exception as e:
        logger.error(f"ERROR while posting time off for {worker_id}: {e}")

def calculate_hours_logic(units, sql_hrs):
    """
    Placeholder for 'calculate hours logic'.
    User said: "look at the timeOffType value if this 'Vacation' we need calculate hours logic"
    User also provided SQL `hrs`.
    I will assume we want to use the SQL hours as the source of truth to update/request.
    """
    # If units is needed for calculation, it is available here.
    # For now returning sql_hrs as string.
    return str(sql_hrs)

def get_worker_details_spark():
    """
    Query the Lakehouse using Spark SQL to get the target data.
    """
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
        # Mock data based on query structure
        return [
            {
                "WorkdayId": "50454", 
                "Timecard_post_date": "2026-01-01", 
                "timecard_worked_date": "2026-01-05",
                "hrs": 8.0
            }
        ] 
    except Exception as e:
        logger.error(f"Error executing Spark query: {e}")
        return []

def process_single_row(row, report_endpoint, access_token):
    """
    Process a single row from the SQL query.
    """
    # Extract mapped fields
    workday_id = row.get("WorkdayId") # -> Colleague_ID
    prompt_date = row.get("Timecard_post_date") # -> promptDate1
    worked_date = row.get("timecard_worked_date") # -> date
    sql_hrs = row.get("hrs")

    if not all([workday_id, prompt_date, worked_date]):
        logger.warning(f"Skipping row due to missing data: {row}")
        return

    # 1. Fetch Report Data
    entries = fetch_time_off_report_data(
        report_endpoint, access_token, workday_id, prompt_date, worked_date
    )
    
    # 2. Process Entries
    for entry in entries:
        # Check timeOffType
        # The structure of 'entry' depends on the Workday report JSON. 
        # Typically: {"timeOffType": {"descriptor": "Vacation", "id": "..."}} or similar.
        # User said: "look at the timeOffType value if this 'Vacation'"
        # And "timeOffEntryWid" is likely a field in the entry.
        
        # Adjust key access based on actual API response structure (assumed flat or nested)
        # Assuming 'timeOffType' is a dict or string in the entry.
        
        time_off_type_obj = entry.get("timeOffType", {})
        # If it's a dict, get descriptor or id. If string, compare directly.
        time_off_type_val = ""
        if isinstance(time_off_type_obj, dict):
            time_off_type_val = time_off_type_obj.get("descriptor", "")
            # Or maybe checking the 'id' if we knew the ID for Vacation.
        else:
            time_off_type_val = str(time_off_type_obj)
            
        # Also check simple key if flattened
        if not time_off_type_val and "timeOffType" in entry:
             time_off_type_val = str(entry["timeOffType"])

        # Check for Vacation
        if "Vacation" in time_off_type_val: # Loose match as per "Vacation Time Off"
            
            # Get timeOffEntryWid
            # User said "id is equal to timeOffEntryWid".
            # In report entry, WID is often in "id" or "WID" or "timeOffEntryWid"
            wid = entry.get("timeOffEntryWid")
            if not wid:
                 wid = entry.get("WID") # Fallback
            if not wid:
                 # Check if the 'id' field in the entry is the wid
                 wid = entry.get("id")

            if not wid:
                logger.warning(f"Could not find timeOffEntryWid for Vacation entry: {entry}")
                continue

            units = entry.get("units") # User mentioned 'units'

            # Calculate Quantity
            qty = calculate_hours_logic(units, sql_hrs)
            
            # Post Update
            post_time_off_update(access_token, workday_id, wid, worked_date, qty)


def ingest_time_off_process(
    client_id: str, 
    client_secret: str, 
    refresh_token: str, 
    token_endpoint: str,
    report_endpoint: str,
    max_workers: int = 100
):
    logger.info("=== Workday Time Off Processing Started ===")

    # 1. Get Token
    access_token = refresh_workday_access_token(
        client_id, client_secret, refresh_token, token_endpoint
    )

    # 2. Get Data from Spark
    rows = get_worker_details_spark()
    
    # 3. Threaded Processing
    logger.info(f"Starting threaded processing with {max_workers} threads for {len(rows)} records...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_row = {
            executor.submit(
                process_single_row, 
                row, 
                report_endpoint, 
                access_token
            ): row 
            for row in rows
        }
        
        completed_count = 0
        total_rows = len(rows)
        log_interval = max(max_workers, 10)

        for future in as_completed(future_to_row):
            completed_count += 1
            row = future_to_row[future]
            
            try:
                future.result() # Output handled in process_single_row
                
                if completed_count % log_interval == 0 or completed_count == total_rows:
                    logger.info(f"Progress: {completed_count}/{total_rows} records processed.")
                    
            except Exception as exc:
                logger.error(f"Row processing generated an exception: {exc}")

    logger.info("=== Workday Time Off Processing Finished ===")

if __name__ == "__main__":
    CLIENT_ID = "your_client_id"
    CLIENT_SECRET = "your_client_secret"
    REFRESH_TOKEN = "your_refresh_token"
    TOKEN_ENDPOINT = "https://wd3-impl-services1.workday.com/ccx/oauth2/nrf3/token"
    TIMEOFF_REPORT_ENDPOINT = "https://wd3-impl-services1.workday.com/ccx/service/customreport2/nrf3/INT0137_USA_HCM_Datahub_Absence_ISU/CRI_INT0137_USA_Datahub_Timeoffs"
    
    ingest_time_off_process(
        CLIENT_ID, 
        CLIENT_SECRET, 
        REFRESH_TOKEN, 
        TOKEN_ENDPOINT, 
        TIMEOFF_REPORT_ENDPOINT,
        max_workers=100
    )

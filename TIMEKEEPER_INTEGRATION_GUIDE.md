# Timekeeper Integration: Fabric/Sierra → Workday

## Overview

This integration synchronizes time-off data from the on-premises data platform (Fabric/Sierra) to Workday using REST APIs. The system handles vacation time, sick time, and lawyer supplement time off for Associates and Counsels.

---

## 1. Enter Time Off API

### What It Is
Creates a **new** time-off entry in Workday for a worker on a specific date.

### Endpoint
```
POST /ccx/api/absenceManagement/v3/{tenant}/workers/{workerWid}/requestTimeOff
```

### When to Use
- **INSERT (I)** operation from source system
- First time recording time-off for a worker on a specific date
- No existing entry exists for that worker + date + type combination

### Required Inputs

| Field | Source | Example |
|-------|--------|---------|
| `workerWid` | Fabric worker table (WID column) | `f503c098b21d10010654c7866af00003` |
| `date` | SQL query `TRAN_DATE` | `2025-03-14T08:00:00.000Z` |
| `dailyQuantity` | SQL query `TOBILL_HRS` | `8` (hours) or `1` (days) |
| `timeOffType.id` | Fixed WID per type | See Time Off Types below |

### Request Payload
```json
{
  "days": [
    {
      "dailyQuantity": "8",
      "comment": "INT0137",
      "timeOffType": {
        "descriptor": "Vacation Time Off",
        "id": "c26e6ef1f81f01e6aae5eddb61890000"
      },
      "date": "2025-03-14T08:00:00.000Z"
    }
  ]
}
```

### What Happens in Workday
1. Workday validates worker exists and is active
2. Validates time-off type is valid for worker's absence plan
3. Validates quantity doesn't exceed daily maximum (based on worker's schedule)
4. Creates time-off entry with status "Submitted" or "Approved" (based on config)
5. Updates worker's time-off balance
6. Returns the created entry with its WID (Time Off Entry ID)

### Response
```json
{
  "days": [
    {
      "id": "d4b6062ffea51001206c41c59d0a0000",
      "descriptor": "Time Off Entry",
      "dailyQuantity": "8",
      "date": "2025-03-14",
      "timeOffType": {
        "id": "c26e6ef1f81f01e6aae5eddb61890000",
        "descriptor": "Vacation Time Off"
      }
    }
  ],
  "businessProcessParameters": {
    "transactionStatus": {
      "descriptor": "Successfully Completed"
    }
  }
}
```

---

## 2. Time Off Types (Vacation vs Sick vs Lawyer Supplement)

### NOT Separate APIs
All time-off types use the **same API endpoint**. The difference is the `timeOffType.id` in the payload.

### Time Off Type WIDs (Fixed Values)

| Type | Descriptor | WID (id) | Unit |
|------|------------|----------|------|
| Vacation | Vacation Time Off | `c26e6ef1f81f01e6aae5eddb61890000` | Hours |
| Sick | Sick Time Off | `{sick_time_wid}` | Hours |
| Lawyer Supplement | Lawyer Supplement Time Off | `{lawyer_supplement_wid}` | Days |

### How to Determine Type
Source system determines type based on **Matter Code**:

```python
VACATION_MATTERS = ['8000000028', '1000325429', '8000000016', '1000325434', '1000086654']
SICK_MATTERS = ['matter_code_for_sick_1', 'matter_code_for_sick_2']

if matter_code in VACATION_MATTERS:
    time_off_type = "Vacation"
elif matter_code in SICK_MATTERS:
    time_off_type = "Sick"
```

### Payload Difference (Only `timeOffType` Changes)

**Vacation:**
```json
{
  "timeOffType": {
    "descriptor": "Vacation Time Off",
    "id": "c26e6ef1f81f01e6aae5eddb61890000"
  }
}
```

**Sick:**
```json
{
  "timeOffType": {
    "descriptor": "Sick Time Off",
    "id": "{sick_time_wid}"
  }
}
```

---

## 3. Correct Time Off Entry API

### What It Is
Modifies an **existing** time-off entry. Used to zero out or adjust entries.

### Endpoint
```
POST /ccx/api/absenceManagement/v3/{tenant}/workers/{workerWid}/correctTimeOffEntry
```

### When Adjustments Are Required
- **UPDATE (U)** operation from source system
- Hours changed for existing entry
- Entry needs to be removed (set to 0)
- Correction of previous entry

### Why Time Entry ID is Mandatory
Workday needs to know **which specific entry** to correct. Multiple entries can exist for:
- Same worker on same date (different types)
- Same worker, same date, same type (corrections create new entries)

### How to Get Time Entry ID (RAS Report)

**Step 1:** Call the custom RAS report to get existing entries:
```
GET /ccx/service/customreport2/{tenant}/INT0137_USA_HCM_Datahub_Absence_ISU/CRI_INT0137_USA_Datahub_Timeoffs
```

**Parameters:**
| Param | Description | Example |
|-------|-------------|---------|
| `Colleague_ID` | Worker's colleague ID | `52107` |
| `promptDate1` | Post date from source | `2025-03-17-08:00` |
| `date` | Worked date | `2025-03-14-08:00` |

**Response:**
```json
{
  "Report_Entry": [
    {
      "Colleague_ID": "52107",
      "Time_Off_Completed_Details_group": [
        {
          "timeOffEntryWid": "d4b6062ffea51001206c41c59d0a0000",  // <-- This is the Time Entry ID
          "timeOffType": "Vacation Time Off",
          "units": "8",
          "date": "2025-03-14"
        }
      ]
    }
  ]
}
```

### Request Payload
```json
{
  "days": [
    {
      "dailyQuantity": "0",
      "comment": "INT0137",
      "correctedEntry": {
        "id": "d4b6062ffea51001206c41c59d0a0000"
      },
      "descriptor": "Correct entry",
      "id": "d4b6062ffea51001206c41c59d0a0000"
    }
  ]
}
```

### What CAN Be Changed
- `dailyQuantity` (hours/days)
- `comment`

### What CANNOT Be Changed
- `date` (must create new entry for different date)
- `timeOffType` (must create new entry for different type)
- `worker` (obviously)

---

## 4. End-to-End Processing Logic

### Decision Rules: Enter vs Correct

```
┌─────────────────────────────────────────────────────────────────┐
│                    Source Record                                 │
│  InsertUpdate = ?                                                │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              │                               │
         InsertUpdate = "I"             InsertUpdate = "U"
              │                               │
              ▼                               ▼
    ┌─────────────────┐           ┌─────────────────────────┐
    │ ENTER TIME OFF  │           │ 1. Get RAS Report       │
    │ (New Entry)     │           │ 2. Get Time Entry WID   │
    └─────────────────┘           │ 3. CORRECT to 0         │
                                  │ 4. ENTER new value      │
                                  └─────────────────────────┘
```

### Processing Flow (Pseudocode)

```python
def process_time_off(record):
    # 1. Get required IDs
    colleague_id = record["WorkdayId"]      # e.g., "52107"
    worker_wid = record["WorkerWid"]        # e.g., "f503c098b21d10010654c7866af00003"
    worked_date = record["timecard_worked_date"]
    sql_hrs = record["hrs"]
    insert_update = record["InsertUpdate"]  # "I" or "U"
    
    # 2. Get existing entries from RAS report
    existing_entries = call_ras_report(colleague_id, worked_date)
    
    # 3. Find matching time-off type entry
    matching_entry = find_vacation_entry(existing_entries)
    
    if not matching_entry:
        log_error("No existing entry found")
        return
    
    time_off_entry_wid = matching_entry["timeOffEntryWid"]
    existing_units = matching_entry["units"]
    
    # 4. Process based on Insert/Update flag
    if insert_update == "I":
        # Simple insert - enter time off
        enter_time_off(worker_wid, worked_date, sql_hrs)
    
    elif insert_update == "U":
        # Update requires: zero out first, then re-enter
        
        # sql_hrs is NEGATIVE for updates (reduction amount)
        new_quantity = float(existing_units) + float(sql_hrs)
        
        # Step 1: Zero out existing entry
        correct_time_off(worker_wid, time_off_entry_wid, quantity=0)
        
        # Step 2: Re-enter with new value (if not complete removal)
        if new_quantity > 0:
            enter_time_off(worker_wid, worked_date, new_quantity)
```

### Why Zero Out First?
Workday REST API **cannot directly update** an existing time-off entry's quantity. The workaround is:
1. Correct the existing entry to 0 (effectively removes it)
2. Enter a new entry with the correct value

---

## 5. Source vs Target Responsibilities

### Source System (Fabric/Sierra) Provides

| Data | Description | Example |
|------|-------------|---------|
| `WorkdayId` | Colleague ID | `52107` |
| `WorkerWid` | Worker WID for API URL | `f503c098b21d10010654c7866af00003` |
| `timecard_worked_date` | Date of time off | `2025-03-14` |
| `Timecard_post_date` | When entry was posted | `2025-03-17` |
| `hrs` | Hours (positive for I, negative for U) | `8.0` or `-4.0` |
| `InsertUpdate` | Operation type | `I` or `U` |
| `MATTER_CODE` | Determines time-off type | `8000000028` |

### SQL Query Structure
```sql
SELECT
    HP.EMPLOYEE_CODE       AS WorkdayId,
    WD.WID                 AS WorkerWid,      -- From worker table
    TT.TOBILL_HRS          AS hrs,
    TRAN_DATE              AS timecard_worked_date,
    POST_DATE              AS Timecard_post_date,
    MATTER_CODE,
    CASE 
        WHEN min(POST_DATE) OVER (PARTITION BY HP.INTERNAL_NUM, TRAN_DATE) = POST_DATE 
        THEN 'I' 
        ELSE 'U' 
    END AS InsertUpdate
FROM silver.TAT_TIME TT
JOIN silver.HBM_PERSNL HP ON HP.EMPL_UNO = TT.TK_EMPL_UNO
JOIN silver.WORKER_TABLE WD ON WD.COLLEAGUE_ID = HP.EMPLOYEE_CODE
WHERE MATTER_CODE IN ('vacation_matters', 'sick_matters')
```

### Workday Controls and Validates

| Validation | Description |
|------------|-------------|
| Worker exists | Worker WID must exist and be active |
| Absence plan | Worker must be enrolled in absence plan for that type |
| Daily maximum | Cannot exceed scheduled hours per day |
| Balance available | Some plans check available balance |
| Date validity | Date must be valid (not too far in past/future) |
| Duplicate prevention | Some configs prevent duplicate entries |

---

## 6. Common Pitfalls and Best Practices

### Typical Mistakes to Avoid

| Mistake | Problem | Solution |
|---------|---------|----------|
| Using Colleague ID in URL | API returns "not found" | Use Worker WID (`f503c...`) |
| Not zeroing out before update | Cannot modify existing entry | Always correct to 0 first |
| Wrong time-off type WID | Entry created with wrong type | Use correct fixed WID per type |
| Missing Time Entry WID | Cannot correct entry | Always call RAS report first |
| Sending hours for Days-based types | Incorrect quantity | Convert hours to days for Lawyer Supplement |
| Not handling negative hours | Update logic breaks | Negative = reduction amount |

### Recommended Logging Strategy

```python
# Log at each step
logger.info(f"Processing worker {colleague_id} for date {worked_date}")
logger.info(f"Operation: {insert_update}, SQL Hours: {sql_hrs}")

# Log API requests
logger.info(f"[GET] RAS Report - Colleague: {colleague_id}, Date: {worked_date}")
logger.info(f"[POST] Enter Time Off - Worker WID: {worker_wid}")
logger.info(f"Request Payload: {json.dumps(payload, indent=2)}")

# Log API responses
logger.info(f"Response Status: {status_code}")
logger.info(f"Response Body: {json.dumps(response, indent=2)}")

# Log results
logger.info(f"Result: {'SUCCESS' if success else 'FAILED'}")
if error:
    logger.error(f"Error: {error}")
```

### Error Handling Strategy

```python
def process_with_retry(worker_wid, payload, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = call_api(worker_wid, payload)
            
            if response.status_code in (200, 201):
                return {"success": True, "data": response.json()}
            
            elif response.status_code == 400:
                # Bad request - don't retry, log and skip
                return {"success": False, "error": response.text, "retry": False}
            
            elif response.status_code == 401:
                # Token expired - refresh and retry
                refresh_token()
                continue
            
            elif response.status_code == 404:
                # Worker not found - log and skip
                return {"success": False, "error": "Worker not found", "retry": False}
            
            elif response.status_code >= 500:
                # Server error - retry with backoff
                time.sleep(2 ** attempt)
                continue
                
        except Exception as e:
            logger.error(f"Request failed: {e}")
            time.sleep(2 ** attempt)
    
    return {"success": False, "error": "Max retries exceeded"}
```

### Tracking Table Schema

```sql
CREATE TABLE workday_time_off_logs (
    id                  BIGINT IDENTITY,
    colleague_id        STRING,
    worker_wid          STRING,
    request_date        DATE,
    time_off_type       STRING,
    time_off_entry_wid  STRING,
    operation           STRING,      -- INSERT, UPDATE, REMOVED
    existing_units      DECIMAL,
    sql_hrs             DECIMAL,
    calculated_quantity DECIMAL,
    success             BOOLEAN,
    error               STRING,
    timestamp           TIMESTAMP,
    dry_run             BOOLEAN
)
```

---

## 7. API Reference Summary

| Operation | Endpoint | When to Use |
|-----------|----------|-------------|
| Get Token | `POST /oauth2/{tenant}/token` | Before any API call |
| Get Existing Entries | `GET /customreport2/{tenant}/.../CRI_INT0137_USA_Datahub_Timeoffs` | Before INSERT or UPDATE |
| Enter Time Off | `POST /workers/{workerWid}/requestTimeOff` | New entry (I) or re-enter after zero |
| Correct Time Off | `POST /workers/{workerWid}/correctTimeOffEntry` | Zero out existing entry (U) |

---

## 8. Quick Reference: ID Types

| ID Type | Format | Where Used | Source |
|---------|--------|------------|--------|
| Colleague ID | `52107` | RAS report param | SQL query `EMPLOYEE_CODE` |
| Worker WID | `f503c098b21d10010654c7866af00003` | API URL path | Fabric worker table |
| Time Off Entry WID | `d4b6062ffea51001206c41c59d0a0000` | Correct payload | RAS report response |
| Time Off Type WID | `c26e6ef1f81f01e6aae5eddb61890000` | Enter payload | Fixed per type |

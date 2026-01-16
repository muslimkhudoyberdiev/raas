# Timekeeper Integration Project - Complete Implementation Guide

## Project Overview

**Purpose:** Synchronize time-off data from Sierra (on-premises data platform via Fabric) to Workday using REST APIs.

**Scope:** 
- Vacation Time Off
- Sick Time Off  
- Lawyer Supplement Time Off

**Target Users:** Associates and Counsels at specific US offices.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SOURCE: Microsoft Fabric                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────┐      ┌─────────────────────────────────┐   │
│  │  US_IT_FINANCE_LH_L1        │      │  US_IT_HRIS_LH_L0_LakeHouse     │   │
│  │  (Sierra Data)              │      │  (Workday Worker Data)          │   │
│  │                             │      │                                 │   │
│  │  sc_bronze.HBM_MATTER       │      │  workday_batch_worker_details   │   │
│  │  sc_bronze.TAT_TIME         │      │  - workdayId (Worker WID)       │   │
│  │  sc_bronze.HBM_PERSNL       │      │  - colleagueId                  │   │
│  │  sc_bronze.HBM_CLIENT       │      │                                 │   │
│  │  sc_bronze.HBL_DEPT         │      └─────────────────────────────────┘   │
│  │  sc_bronze.HBL_OFFICE       │                    │                       │
│  └─────────────────────────────┘                    │                       │
│               │                                     │                       │
│               └──────────────┬──────────────────────┘                       │
│                              │ JOIN on colleagueId                          │
│                              ▼                                              │
│                    ┌─────────────────┐                                      │
│                    │   Spark Query   │                                      │
│                    │   (Combined)    │                                      │
│                    └────────┬────────┘                                      │
└─────────────────────────────┼───────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          INTEGRATION LAYER                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. Get OAuth Token                                                          │
│  2. For each record:                                                         │
│     a. Call RAS Report (GET existing time-off entries)                       │
│     b. Determine INSERT or UPDATE                                            │
│     c. If UPDATE: correctTimeOffEntry (zero out) → requestTimeOff (new val) │
│     d. If INSERT: requestTimeOff                                             │
│  3. Log results to tracking table                                            │
│                                                                              │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TARGET: Workday                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  REST APIs:                                                                  │
│  - POST /oauth2/{tenant}/token                    (Authentication)           │
│  - GET  /customreport2/.../CRI_INT0137_...        (RAS Report)              │
│  - POST /workers/{workerWid}/requestTimeOff       (Enter Time Off)          │
│  - POST /workers/{workerWid}/correctTimeOffEntry  (Correct/Zero Out)        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Source Query Output

| Field | Description | Example |
|-------|-------------|---------|
| `ColleagueId` | Employee ID (for RAS report) | `52107` |
| `WorkerWid` | Workday Worker WID (for API URL) | `f503c098b21d10010654c7866af00003` |
| `timecard_worked_date` | Date of time off | `2025-03-14` |
| `Timecard_post_date` | When entry was posted | `2025-03-17` |
| `hrs` | Hours (positive=INSERT, negative=UPDATE) | `8.0` or `-4.0` |
| `InsertUpdate` | Operation type | `I` or `U` |
| `MATTER_CODE` | Determines time-off type | `8000000028` |

### Matter Code → Time Off Type Mapping

| Matter Codes | Time Off Type | WID | Unit |
|--------------|---------------|-----|------|
| `8000000028`, `1000325429`, `8000000016`, `1000325434`, `1000086654` | Vacation Time Off | `c26e6ef1f81f01e6aae5eddb61890000` | Hours |
| TBD | Sick Time Off | TBD | Hours |
| TBD | Lawyer Supplement Time Off | TBD | Days |

---

## API Specifications

### 1. Authentication - Get OAuth Token

```
POST https://wd3-impl-services1.workday.com/ccx/oauth2/nrf3/token
```

**Headers:**
```
Authorization: Basic {base64(client_id:client_secret)}
Content-Type: application/x-www-form-urlencoded
```

**Body:**
```
grant_type=refresh_token&refresh_token={refresh_token}
```

**Response:**
```json
{
  "access_token": "eyJ...",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

---

### 2. RAS Report - Get Existing Time Off Entries

```
GET https://wd3-impl-services1.workday.com/ccx/service/customreport2/nrf3/INT0137_USA_HCM_Datahub_Absence_ISU/CRI_INT0137_USA_Datahub_Timeoffs
```

**Parameters:**
| Param | Description | Example |
|-------|-------------|---------|
| `Colleague_ID` | Employee ID | `52107` |
| `promptDate1` | Post date | `2025-03-17-08:00` |
| `date` | Worked date | `2025-03-14-08:00` |
| `Include_Terminated_Workers` | Include terminated | `0` |
| `format` | Response format | `json` |

**Response:**
```json
{
  "Report_Entry": [
    {
      "Colleague_ID": "52107",
      "Worker": "John Smith (52107)",
      "Time_Off_Completed_Details_group": [
        {
          "timeOffEntryWid": "d4b6062ffea51001206c41c59d0a0000",
          "timeOffType": "Vacation Time Off",
          "units": "8",
          "date": "2025-03-14",
          "createdMoment": "2025-03-17T07:06:58.443-07:00"
        }
      ],
      "vacBal": "120",
      "sickBal": "40"
    }
  ]
}
```

---

### 3. Enter Time Off (requestTimeOff)

```
POST https://wd3-impl-services1.workday.com/ccx/api/absenceManagement/v3/nrf3/workers/{workerWid}/requestTimeOff
```

**URL Parameters:**
- `workerWid`: Worker WID (e.g., `f503c098b21d10010654c7866af00003`)

**Headers:**
```
Authorization: Bearer {access_token}
Content-Type: application/json
```

**Request Body:**
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

**Response:**
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

### 4. Correct Time Off Entry (correctTimeOffEntry)

Used to zero out an existing entry before re-entering with new value.

```
POST https://wd3-impl-services1.workday.com/ccx/api/absenceManagement/v3/nrf3/workers/{workerWid}/correctTimeOffEntry
```

**Request Body:**
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

---

## Processing Logic

### Decision Flow

```
┌─────────────────────────────────────┐
│         Source Record               │
│         InsertUpdate = ?            │
└─────────────────────────────────────┘
                  │
    ┌─────────────┴─────────────┐
    │                           │
    ▼                           ▼
┌───────────┐           ┌───────────────┐
│   "I"     │           │     "U"       │
│ (INSERT)  │           │   (UPDATE)    │
└─────┬─────┘           └───────┬───────┘
      │                         │
      │                         ▼
      │                 ┌───────────────────┐
      │                 │ 1. Get RAS Report │
      │                 │ 2. Get Entry WID  │
      │                 │ 3. Calculate:     │
      │                 │    new = old + hrs│
      │                 │    (hrs is neg)   │
      │                 └─────────┬─────────┘
      │                           │
      │               ┌───────────┴───────────┐
      │               │                       │
      │               ▼                       ▼
      │       ┌───────────────┐       ┌───────────────┐
      │       │ new_qty > 0   │       │ new_qty <= 0  │
      │       └───────┬───────┘       └───────┬───────┘
      │               │                       │
      │               ▼                       ▼
      │       ┌───────────────┐       ┌───────────────┐
      │       │ correctEntry  │       │ correctEntry  │
      │       │ (zero out)    │       │ (zero out)    │
      │       │      +        │       │    DONE       │
      │       │ requestTimeOff│       │   (removed)   │
      │       │ (new value)   │       └───────────────┘
      │       └───────────────┘
      │               │
      └───────────────┤
                      ▼
              ┌───────────────┐
              │ requestTimeOff│
              │ (enter value) │
              └───────────────┘
```

### Units Calculation

**INSERT (I):**
```python
quantity = sql_hrs  # Use hours directly
```

**UPDATE (U):**
```python
# sql_hrs is NEGATIVE (reduction amount)
new_quantity = existing_units + sql_hrs

# Example: existing=8, sql_hrs=-4 → new_quantity=4
# Example: existing=8, sql_hrs=-8 → new_quantity=0 (removal)
```

---

## Fabric Setup

### Cross-Lakehouse Access

**Problem:** Finance and HRIS data are in different lakehouses.

**Solution Options:**

1. **Create Shortcut (Recommended)**
   - In `US_IT_FINANCE_LH_L1` → Tables → New shortcut
   - Link to `US_IT_HRIS_LH_L0_LakeHouse.workday_batch_worker_details`
   - Now accessible as `workday_batch_worker_details` from Finance

2. **Copy Worker Mapping Table**
   - One-time copy of worker WID mapping to Finance lakehouse
   - Use in JOIN queries

3. **Use abfss:// Path**
   - Read directly using storage path:
   ```python
   spark.read.format("delta").load("abfss://...path...")
   ```

### SQL Query Structure

```sql
SELECT
    HP.EMPLOYEE_CODE       AS ColleagueId,
    WBW.workdayId          AS WorkerWid,
    TT.TOBILL_HRS          AS hrs,
    TRAN_DATE              AS timecard_worked_date,
    POST_DATE              AS Timecard_post_date,
    MATTER_CODE,
    CASE 
        WHEN min(POST_DATE) OVER (PARTITION BY HP.INTERNAL_NUM, TRAN_DATE) = POST_DATE 
        THEN 'I' 
        ELSE 'U' 
    END AS InsertUpdate,
    HP.`POSITION`          AS jobtitle
FROM sc_bronze.HBM_MATTER M
JOIN sc_bronze.TAT_TIME TT ON M.MATTER_UNO = TT.MATTER_UNO
JOIN sc_bronze.HBM_CLIENT HC ON HC.CLIENT_UNO = M.CLIENT_UNO
JOIN sc_bronze.HBM_PERSNL HP ON HP.EMPL_UNO = TT.TK_EMPL_UNO
JOIN sc_bronze.HBL_DEPT HD ON HD.DEPT_CODE = HP.DEPT
JOIN sc_bronze.HBL_OFFICE HO ON HO.OFFC_CODE = HP.OFFC
LEFT JOIN workday_batch_worker_details WBW ON WBW.colleagueId = HP.EMPLOYEE_CODE
WHERE MATTER_CODE IN ('8000000028','1000325429','8000000016','1000325434','1000086654')
  AND year(TRAN_DATE) >= year(current_date()) - 1
  AND HP.`POSITION` IN ('Associate', 'Counsel')
  AND HO.OFFC_CODE IN ('AUS1','CHI1','DAL1','DEN1','HOU1','LAX1','IPS1','NYC1','PIT1','SAT1','SFO1','STL1','WAS1')
  AND lower(HP.`POSITION`) NOT LIKE '%partner%'
```

---

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `"not found: 52107"` | Using ColleagueId in URL instead of WorkerWid | Use `WorkerWid` (e.g., `f503c...`) in API URL |
| `TABLE_OR_VIEW_NOT_FOUND` | Cross-lakehouse access issue | Create shortcut or copy table |
| `Schema mismatch` | Log table schema changed | Add `.option("mergeSchema", "true")` |
| `Token expired` | Access token expired | Refresh token before each batch |

### Retry Strategy

```python
def call_api_with_retry(url, payload, max_retries=3):
    for attempt in range(max_retries):
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code in (200, 201):
            return {"success": True, "data": response.json()}
        elif response.status_code == 401:
            refresh_token()  # Token expired
            continue
        elif response.status_code >= 500:
            time.sleep(2 ** attempt)  # Exponential backoff
            continue
        else:
            return {"success": False, "error": response.text}
    
    return {"success": False, "error": "Max retries exceeded"}
```

---

## Logging & Tracking

### Log Table Schema

```sql
CREATE TABLE workday_time_off_logs (
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

### Console Logging Format

```
================================================================================
PROCESSING WORKER: 52107 | Date: 2025-03-14
================================================================================
  Worker WID: f503c098b21d10010654c7866af00003
  Operation: INSERT | SQL Hours: 8.0
  [GET] RAS Report...
  Response Status: 200
  Response Body: [{"timeOffEntryWid": "...", "units": "8", ...}]
  
  Calculation Details:
    - Operation: INSERT
    - Existing Units: 0
    - SQL Hours: 8.0
    - Calculated Quantity: 8.0
  
  TIME OFF MATCH FOUND:
    - Type: Vacation
    - WID: d4b6062ffea51001206c41c59d0a0000
    - Quantity: 8.0 Hours
  
  [POST] requestTimeOff...
  Result: SUCCESS
  
  Worker 52107 summary: 1 entries processed (Vacation: 1), 0 skipped
```

---

## Configuration

### Environment Variables / Secrets

```python
CLIENT_ID = "your_client_id"
CLIENT_SECRET = "your_client_secret"
REFRESH_TOKEN = "your_refresh_token"
TOKEN_URL = "https://wd3-impl-services1.workday.com/ccx/oauth2/nrf3/token"
REPORT_URL = "https://wd3-impl-services1.workday.com/ccx/service/customreport2/nrf3/INT0137_USA_HCM_Datahub_Absence_ISU/CRI_INT0137_USA_Datahub_Timeoffs"
```

### Time Off Type Configuration

```python
SUPPORTED_TIME_OFF_TYPES = {
    "Vacation": {
        "descriptor": "Vacation Time Off",
        "wid": "c26e6ef1f81f01e6aae5eddb61890000",
        "unit_of_time": "Hours"
    },
    "Lawyer Supplement Time Off": {
        "descriptor": "Lawyer Supplement Time Off",
        "wid": "TBD",
        "unit_of_time": "Days"
    },
    "Sick": {
        "descriptor": "Sick Time Off",
        "wid": "TBD",
        "unit_of_time": "Hours"
    }
}
```

---

## Testing

### Test with Single Record

Add filter to SQL query:
```sql
-- TEST FILTER: Remove after testing
AND HP.EMPLOYEE_CODE = '52107'
AND TRAN_DATE = '2025-03-14'
```

### Dry Run Mode

```python
ingest_time_off_process(
    ...,
    dry_run=True  # No actual API calls
)
```

### Verify in Workday

After processing, verify in Workday:
1. Search for worker by Colleague ID
2. Navigate to Time Off → Time Off History
3. Confirm entry exists with correct hours/date

---

## Production Checklist

- [ ] Worker WID mapping accessible (shortcut or copied table)
- [ ] Time off type WIDs confirmed for all types
- [ ] Matter code mapping complete
- [ ] Remove test filters from SQL query
- [ ] Set `dry_run=False`
- [ ] Configure appropriate `max_workers` (recommend 10-50)
- [ ] Log table created and accessible
- [ ] Error alerting configured
- [ ] Token refresh mechanism working
- [ ] Tested with INSERT and UPDATE scenarios

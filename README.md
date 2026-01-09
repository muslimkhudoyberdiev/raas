# Raas Project

## Updates (2026-01-09)

### Time Off Integration (REST API)
A Python script `time_off_manager.py` has been created to handle the Time Off integration using the new REST API flow as discussed.

**Features:**
- **Authentication**: Retrieves OAuth2 token.
- **Silver Layer Lookup**: Maps Colleague ID to Workday ID (currently mocked).
- **Enter Time Off**: logic to submit new time off requests.
- **Adjust Time Off**: Implements the "Zero-out and Re-enter" logic for updates:
  1. Checks for existing entry using RAS Report.
  2. If exists and hours differ, adjusts existing entry to 0.
  3. Enters new time off request with correct hours.
- **Tracking**: Includes placeholder for updating a tracking table/log with process status.

**Usage:**
```bash
python3 time_off_manager.py
```

### Investigations
The following investigations are pending access to the relevant environments/logs:

1.  **Data Update Delay (Languages Table)**
    - **Issue**: 3-hour delay between JSON file availability (which was correct) and table update.
    - **Action Plan**:
        - Check ETL pipeline logs for the specific run at ~2 PM.
        - Verify if the "Languages" table has a specific dependency or lower priority in the pipeline.
        - Check for any "table lock" or long-running transaction that might have delayed the commit.

2.  **Duplicate Records (Worker Details)**
    - **Issue**: Duplicate rows found for some Colleague IDs.
    - **Observations**:
        - Some duplicates have `leave_of_absence` set, others are null.
        - Key was thought to be `colleague_id` + `hire_date`, but duplicates exist with different hire dates or same `colleague_id`.
    - **Action Plan**:
        - Query `worker_details` for the specific IDs (e.g., `61783`, `50454`) to inspect all columns.
        - Verify the definition of the unique key with the Data Modeling team.
        - Check if "re-hires" or multiple active assignments are causing rows.

import requests
import json
from datetime import datetime
from typing import Optional, Dict, Any

class TimeOffManager:
    def __init__(self, base_url: str, client_id: str, client_secret: str, refresh_token: str):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = None
        
        # Endpoints (Placeholders based on transcript)
        self.token_endpoint = f"{base_url}/oauth2/token"
        self.enter_time_off_endpoint = f"{base_url}/time_off/enter"
        self.adjust_time_off_endpoint = f"{base_url}/time_off/adjust"
        self.ras_report_endpoint = f"{base_url}/reports/ras"
        
        # Mock Silver Layer connection string or configuration
        self.silver_layer_connection = "..." 

    def get_token(self) -> str:
        """
        Retrieves the OAuth2 access token.
        Corresponds to: "A couple of calls, one is the gift token... get token"
        """
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        # In a real scenario, we would make the request:
        # response = requests.post(self.token_endpoint, data=payload)
        # response.raise_for_status()
        # self.access_token = response.json()['access_token']
        
        print(f"Mocking Get Token call to {self.token_endpoint}")
        self.access_token = "mock_access_token_123"
        return self.access_token

    def get_workday_id(self, colleague_id: str) -> Optional[str]:
        """
        Looks up the Workday ID from the Silver Layer (Data Lake/Warehouse) using Colleague ID.
        Transcript: "You grab the Workday ID... query on this lake house. Silver layer... based on the calling ID."
        """
        print(f"Looking up Workday ID for Colleague ID: {colleague_id} in Silver Layer")
        # Mock database lookup
        # In reality: Execute SQL query against Fabric/Lakehouse
        # SELECT workday_id FROM silver.worker_table WHERE colleague_id = ?
        
        mock_mapping = {
            "50454": "F003_WORKDAY_ID", # Example from transcript
            "61783": "W_ID_61783"
        }
        return mock_mapping.get(colleague_id, f"GENERATED_WID_{colleague_id}")

    def get_existing_time_entry(self, colleague_id: str, date: str) -> Optional[Dict[str, Any]]:
        """
        Calls the RAS report to find existing time entries for a given date.
        Transcript: "Before you do that [adjust], you have to call a RAS report... checks positive values."
        """
        if not self.access_token:
            self.get_token()
            
        print(f"Calling RAS Report for {colleague_id} on {date}")
        
        # Mock response
        # In reality: requests.get(self.ras_report_endpoint, params={'colleague_id': colleague_id, 'date': date}, headers=...)
        
        # Simulating a scenario where an entry might exist
        # Returning a dictionary with time_entry_id and hours if found, else None
        return None # Default to no existing entry for basic test

    def enter_time_off(self, workday_id: str, date: str, hours: float, code: str) -> bool:
        """
        Enters a new time off request.
        Transcript: "Call the enter time off... pass this ID [code]... date... hours"
        """
        if not self.access_token:
            self.get_token()
            
        payload = {
            "workday_id": workday_id,
            "date": date,
            "hours": hours,
            "code": code # e.g. Vacation ID or Sick Time ID
        }
        
        print(f"Entering Time Off: {json.dumps(payload, indent=2)}")
        # requests.post(self.enter_time_off_endpoint, json=payload, headers=...)
        return True

    def adjust_time_off_to_zero(self, time_entry_id: str) -> bool:
        """
        Adjusts an existing time entry to zero hours.
        Transcript: "You get to make it to zero on that day... adjust time off... make it to zero"
        """
        if not self.access_token:
            self.get_token()
            
        payload = {
            "time_entry_id": time_entry_id,
            "hours": 0
        }
        
        print(f"Adjusting Time Entry {time_entry_id} to 0 hours")
        # requests.post(self.adjust_time_off_endpoint, json=payload, headers=...)
        return True

    def process_time_off_request(self, colleague_id: str, date: str, hours: float, code: str):
        """
        Main orchestration function.
        Handles the logic of check existing -> adjust (if needed) -> enter new.
        """
        print(f"\n--- Processing Time Off for {colleague_id} on {date} ---")
        
        # 1. Get Token
        self.get_token()
        
        # 2. Get Workday ID
        workday_id = self.get_workday_id(colleague_id)
        if not workday_id:
            print(f"Error: Could not find Workday ID for {colleague_id}")
            return

        # 3. Check for existing entry (RAS Report)
        existing_entry = self.get_existing_time_entry(colleague_id, date)
        
        if existing_entry:
            current_hours = existing_entry.get('hours', 0)
            time_entry_id = existing_entry.get('time_entry_id')
            
            print(f"Found existing entry: {current_hours} hours (ID: {time_entry_id})")
            
            if current_hours != hours:
                # 4a. Adjust existing to zero if needed
                # Transcript: "make it to zero first... and then enter the other end"
                self.adjust_time_off_to_zero(time_entry_id)
                
                # 4b. Enter new hours
                self.enter_time_off(workday_id, date, hours, code)
            else:
                print("Hours match existing entry. No action needed.")
        else:
            # 5. No existing entry, just enter new
            print("No existing entry found. Creating new entry.")
            self.enter_time_off(workday_id, date, hours, code)

        # 6. Update tracking table (as requested in transcript)
        self.update_tracking_table(colleague_id, date, "SUCCESS")

    def update_tracking_table(self, colleague_id: str, date: str, status: str):
        """
        Updates the tracking table in Fabric/Database.
        Transcript: "You may need to roll another column... process date, status."
        """
        print(f"Updating tracking table: {colleague_id} | {date} | {status} | {datetime.now()}")

# Example Usage
if __name__ == "__main__":
    # Configuration
    API_BASE_URL = "https://api.workday.com/ccx/service/custom" # Example
    CLIENT_ID = "your_client_id"
    CLIENT_SECRET = "your_client_secret"
    REFRESH_TOKEN = "your_refresh_token"
    
    manager = TimeOffManager(API_BASE_URL, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
    
    # Test Case 1: New Entry (Vacation)
    # 50454 is from the transcript example
    manager.process_time_off_request(
        colleague_id="50454", 
        date="2023-11-07", 
        hours=8.0, 
        code="VACATION_CODE_123"
    )

    # Test Case 2: Adjustment (Mocking the existence in a subclass or by modifying the method temporarily for demo)
    print("\n[Simulating Adjustment Scenario]")
    # Monkey patching for demonstration purposes
    def mock_get_existing(colleague_id, date):
        return {"time_entry_id": "EXISTING_ENTRY_999", "hours": 8.0}
    
    manager.get_existing_time_entry = mock_get_existing
    
    # User changes 8 hours to 4 hours
    manager.process_time_off_request(
        colleague_id="50454", 
        date="2023-11-07", 
        hours=4.0, 
        code="VACATION_CODE_123"
    )

import requests
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
from dotenv import load_dotenv

# ============================================================================
# CONFIGURATION - Update these with your credentials
# ============================================================================
# Load environment variables
load_dotenv()
API_TOKEN = os.getenv('NOLOCO_API_TOKEN')
PROJECT_ID = os.getenv('NOLOCO_PROJECT_ID')
API_URL = f"https://api.portals.noloco.io/data/{PROJECT_ID}"

# Add validation
if not API_TOKEN:
    raise Exception("ERROR: NOLOCO_API_TOKEN environment variable not set!")
if not PROJECT_ID:
    raise Exception("ERROR: NOLOCO_PROJECT_ID environment variable not set!")

# HTTP headers for API requests
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}


class NolocoPayrollAutomation:
    """
    Automates payroll record generation from approved timesheets in Noloco.
    Handles one-to-many relationship between payroll periods and timesheets.
    """
    
    def __init__(self):
        """Initialize with global configuration."""
        self.api_url = API_URL
        self.headers = HEADERS
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                     params: Optional[Dict] = None) -> Dict:
        """
        Make HTTP request to Noloco API with error handling.
        
        Args:
            method: HTTP method (GET, POST, PATCH, DELETE)
            endpoint: Collection name (e.g., 'timesheets', 'payroll')
            data: Request payload for POST/PATCH
            params: Query parameters for filtering
            
        Returns:
            Response JSON data
        """
        url = f"{self.api_url}/{endpoint}"
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=self.headers, params=params)
            elif method == 'POST':
                response = requests.post(url, headers=self.headers, json=data)
            elif method == 'PATCH':
                response = requests.patch(url, headers=self.headers, json=data)
            elif method == 'DELETE':
                response = requests.delete(url, headers=self.headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f"❌ API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            raise
    
    def get_all_records(self, collection: str, filters: Optional[Dict] = None) -> List[Dict]:
        """
        Retrieve all records from a collection with optional filtering.
        Handles pagination automatically.
        
        Args:
            collection: Collection name (e.g., 'timesheets', 'payroll', 'employees')
            filters: Optional filter dictionary
            
        Returns

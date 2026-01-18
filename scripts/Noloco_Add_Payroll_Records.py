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
            
        Returns:
            List of all records
        """
        all_records = []
        page = 1
        page_size = 100
        
        while True:
            params = {
                'page': page,
                'limit': page_size
            }
            
            # Add filters if provided
            if filters:
                params.update(filters)
            
            try:
                response = self._make_request('GET', collection, params=params)
                
                # Handle different response structures
                if isinstance(response, list):
                    records = response
                elif isinstance(response, dict):
                    records = response.get('data', response.get('records', []))
                else:
                    records = []
                
                if not records:
                    break
                
                all_records.extend(records)
                
                # Check if we've retrieved all records
                if len(records) < page_size:
                    break
                
                page += 1
                
            except Exception as e:
                print(f"Error fetching page {page}: {e}")
                break
        
        return all_records
    
    def get_approved_timesheets(self) -> List[Dict]:
        """
        Retrieve all approved timesheets that haven't been processed for payroll.
        
        Returns:
            List of approved timesheet records
        """
        print("📋 Fetching approved timesheets...")
        
        # Get all timesheets
        all_timesheets = self.get_all_records('timesheets')
        
        # Filter for approved and not processed
        # Adjust field names based on your actual Noloco schema
        approved_timesheets = [
            ts for ts in all_timesheets
            if ts.get('approved') == True and ts.get('payroll_processed') != True
        ]
        
        print(f"   Found {len(approved_timesheets)} approved, unprocessed timesheet(s)")
        return approved_timesheets
    
    def get_payroll_records(self, employee_id: Optional[str] = None) -> List[Dict]:
        """
        Retrieve payroll records, optionally filtered by employee.
        
        Args:
            employee_id: Optional employee ID to filter by
            
        Returns:
            List of payroll records
        """
        all_payroll = self.get_all_records('payroll')
        
        if employee_id:
            all_payroll = [p for p in all_payroll if p.get('employee_id') == employee_id]
        
        return all_payroll
    
    def determine_pay_period(self, timesheet_date: str) -> Dict[str, str]:
        """
        Determine pay period start and end dates for a given timesheet date.
        Assumes semi-monthly pay periods (1st-15th and 16th-end of month).
        
        Args:
            timesheet_date: Date string from timesheet (ISO format)
            
        Returns:
            Dict with 'start_date' and 'end_date'
        """
        # Parse the date
        try:
            if 'T' in timesheet_date:
                dt = datetime.fromisoformat(timesheet_date.replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(timesheet_date, '%Y-%m-%d')
        except Exception as e:
            print(f"⚠️  Warning: Could not parse date {timesheet_date}: {e}")
            dt = datetime.now()
        
        # Determine pay period (semi-monthly: 1-15 and 16-end)
        if dt.day <= 15:
            period_start = dt.replace(day=1)
            period_end = dt.replace(day=15)
        else:
            period_start = dt.replace(day=16)
            # Get last day of month
            next_month = dt.replace(day=28) + timedelta(days=4)
            period_end = next_month - timedelta(days=next_month.day)
        
        return {
            'start_date': period_start.strftime('%Y-%m-%d'),
            'end_date': period_end.strftime('%Y-%m-%d')
        }
    
    def find_existing_payroll(self, employee_id: str, pay_period: Dict[str, str]) -> Optional[Dict]:
        """
        Find existing payroll record for employee and pay period.
        
        Args:
            employee_id: Employee ID
            pay_period: Dict with start_date and end_date
            
        Returns:
            Existing payroll record or None
        """
        payroll_records = self.get_payroll_records(employee_id)
        
        for record in payroll_records:
            # Match on pay period dates
            if (record.get('pay_period_start') == pay_period['start_date'] and
                record.get('pay_period_end') == pay_period['end_date']):
                return record
        
        return None
    
    def calculate_payroll_totals(self, timesheets: List[Dict]) -> Dict:
        """
        Calculate total hours and pay from timesheets.
        
        Args:
            timesheets: List of timesheet records
            
        Returns:
            Dict with calculated totals
        """
        total_hours = 0.0
        total_regular_hours = 0.0
        total_overtime_hours = 0.0
        total_gross_pay = 0.0
        
        for ts in timesheets:
            # Adjust field names based on your schema
            total_hours += float(ts.get('total_hours', 0))
            total_regular_hours += float(ts.get('regular_hours', 0))
            total_overtime_hours += float(ts.get('overtime_hours', 0))
            total_gross_pay += float(ts.get('gross_pay', 0))
        
        return {
            'total_hours': round(total_hours, 2),
            'regular_hours': round(total_regular_hours, 2),
            'overtime_hours': round(total_overtime_hours, 2),
            'gross_pay': round(total_gross_pay, 2),
            'timesheet_count': len(timesheets)
        }
    
    def create_payroll_record(self, employee_id: str, timesheets: List[Dict], 
                             pay_period: Dict[str, str]) -> Dict:
        """
        Create new payroll record from timesheets.
        
        Args:
            employee_id: Employee ID
            timesheets: List of approved timesheets
            pay_period: Pay period dates
            
        Returns:
            Created payroll record
        """
        totals = self.calculate_payroll_totals(timesheets)
        timesheet_ids = [ts.get('id') for ts in timesheets]
        
        payroll_data = {
            'employee_id': employee_id,
            'pay_period_start': pay_period['start_date'],
            'pay_period_end': pay_period['end_date'],
            'total_hours': totals['total_hours'],
            'regular_hours': totals['regular_hours'],
            'overtime_hours': totals['overtime_hours'],
            'gross_pay': totals['gross_pay'],
            'timesheet_ids': timesheet_ids,
            'status': 'pending',
            'created_date': datetime.now().isoformat(),
            'timesheet_count': totals['timesheet_count']
        }
        
        response = self._make_request('POST', 'payroll', data=payroll_data)
        
        print(f"✅ Created payroll record for employee {employee_id}")
        print(f"   Pay Period: {pay_period['start_date']} to {pay_period['end_date']}")
        print(f"   Total Hours: {totals['total_hours']}, Gross Pay: ${totals['gross_pay']:.2f}")
        print(f"   Timesheets: {totals['timesheet_count']}")
        
        return response
    
    def update_payroll_record(self, payroll_record: Dict, new_timesheets: List[Dict]) -> Dict:
        """
        Update existing payroll record with additional timesheets.
        
        Args:
            payroll_record: Existing payroll record
            new_timesheets: New timesheets to add to this payroll
            
        Returns:
            Updated payroll record
        """
        payroll_id = payroll_record.get('id')
        
        # Get existing timesheet IDs
        existing_timesheet_ids = payroll_record.get('timesheet_ids', [])
        new_timesheet_ids = [ts.get('id') for ts in new_timesheets]
        
        # Combine all timesheet IDs
        all_timesheet_ids = list(set(existing_timesheet_ids + new_timesheet_ids))
        
        # Recalculate totals (you might want to fetch all timesheets here)
        # For now, we'll add the new timesheet totals to existing totals
        new_totals = self.calculate_payroll_totals(new_timesheets)
        
        updated_data = {
            'total_hours': round(payroll_record.get('total_hours', 0) + new_totals['total_hours'], 2),
            'regular_hours': round(payroll_record.get('regular_hours', 0) + new_totals['regular_hours'], 2),
            'overtime_hours': round(payroll_record.get('overtime_hours', 0) + new_totals['overtime_hours'], 2),
            'gross_pay': round(payroll_record.get('gross_pay', 0) + new_totals['gross_pay'], 2),
            'timesheet_ids': all_timesheet_ids,
            'updated_date': datetime.now().isoformat(),
            'timesheet_count': len(all_timesheet_ids)
        }
        
        response = self._make_request('PATCH', f"payroll/{payroll_id}", data=updated_data)
        
        print(f"✅ Updated payroll record {payroll_id}")
        print(f"   Added {len(new_timesheets)} new timesheet(s)")
        print(f"   New Total Hours: {updated_data['total_hours']}, Gross Pay: ${updated_data['gross_pay']:.2f}")
        
        return response
    
    def mark_timesheets_processed(self, timesheet_ids: List[str]):
        """
        Mark timesheets as processed for payroll.
        
        Args:
            timesheet_ids: List of timesheet IDs to mark
        """
        for ts_id in timesheet_ids:
            try:
                self._make_request('PATCH', f"timesheets/{ts_id}", 
                                 data={'payroll_processed': True})
                print(f"   ✓ Marked timesheet {ts_id} as processed")
            except Exception as e:
                print(f"   ⚠️  Warning: Could not mark timesheet {ts_id}: {e}")
    
    def process_timesheets_to_payroll(self):
        """
        Main process: Convert approved timesheets to payroll records.
        Groups by employee and pay period, creates or updates payroll records.
        """
        print("\n" + "="*70)
        print("🚀 STARTING PAYROLL PROCESSING")
        print("="*70 + "\n")
        
        # Get all approved timesheets not yet processed
        approved_timesheets = self.get_approved_timesheets()
        
        if not approved_timesheets:
            print("✓ No approved timesheets to process.\n")
            return
        
        print(f"📊 Processing {len(approved_timesheets)} approved timesheet(s)\n")
        
        # Group timesheets by employee and pay period
        grouped_timesheets = {}
        
        for ts in approved_timesheets:
            employee_id = ts.get('employee_id')
            timesheet_date = ts.get('date') or ts.get('work_date') or ts.get('created_date')
            
            if not employee_id:
                print(f"⚠️  Skipping timesheet {ts.get('id')} - missing employee_id")
                continue
            
            if not timesheet_date:
                print(f"⚠️  Skipping timesheet {ts.get('id')} - missing date")
                continue
            
            pay_period = self.determine_pay_period(timesheet_date)
            key = f"{employee_id}_{pay_period['start_date']}_{pay_period['end_date']}"
            
            if key not in grouped_timesheets:
                grouped_timesheets[key] = {
                    'employee_id': employee_id,
                    'pay_period': pay_period,
                    'timesheets': []
                }
            
            grouped_timesheets[key]['timesheets'].append(ts)
        
        # Process each group
        processed_count = 0
        created_count = 0
        updated_count = 0
        
        for key, group in grouped_timesheets.items():
            employee_id = group['employee_id']
            pay_period = group['pay_period']
            timesheets = group['timesheets']
            
            print(f"\n{'─'*70}")
            print(f"👤 Employee: {employee_id}")
            print(f"📅 Pay Period: {pay_period['start_date']} to {pay_period['end_date']}")
            print(f"📋 Timesheets: {len(timesheets)}")
            
            # Check if payroll record already exists
            existing_payroll = self.find_existing_payroll(employee_id, pay_period)
            
            if existing_payroll:
                # Update existing payroll
                self.update_payroll_record(existing_payroll, timesheets)
                updated_count += 1
            else:
                # Create new payroll record
                self.create_payroll_record(employee_id, timesheets, pay_period)
                created_count += 1
            
            # Mark timesheets as processed
            timesheet_ids = [ts.get('id') for ts in timesheets]
            self.mark_timesheets_processed(timesheet_ids)
            processed_count += len(timesheets)
        
        print("\n" + "="*70)
        print("✅ PAYROLL PROCESSING COMPLETE")
        print("="*70)
        print(f"📊 Summary:")
        print(f"   • Timesheets Processed: {processed_count}")
        print(f"   • Payroll Records Created: {created_count}")
        print(f"   • Payroll Records Updated: {updated_count}")
        print("="*70 + "\n")


def main():
    """
    Main execution function.
    """
    try:
        # Initialize automation
        automation = NolocoPayrollAutomation()
        
        # Run the process
        automation.process_timesheets_to_payroll()
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        raise


if __name__ == "__main__":
    main()

import requests
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
from dotenv import load_dotenv
import time

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

# Configuration for retry logic
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
RATE_LIMIT_DELAY = 0.5  # seconds between record uploads
# ============================================================================


def run_graphql_query(query, retry_count=0):
    """
    Send a GraphQL query to Noloco API and return the response
    Includes retry logic for transient failures
    
    Args:
        query: GraphQL query string
        retry_count: Current retry attempt (internal use)
        
    Returns:
        Response data as dictionary
    """
    try:
        response = requests.post(
            API_URL,
            headers=HEADERS,
            json={"query": query},
            timeout=30
        )
        
        # Handle rate limiting with retry
        if response.status_code == 429:
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_DELAY * (retry_count + 1)
                print(f"  ⚠️  Rate limited, waiting {wait_time}s before retry {retry_count + 1}/{MAX_RETRIES}...")
                time.sleep(wait_time)
                return run_graphql_query(query, retry_count + 1)
            else:
                raise Exception(f"Rate limit exceeded after {MAX_RETRIES} retries")
        
        # Handle server errors with retry
        if response.status_code >= 500:
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_DELAY * (retry_count + 1)
                print(f"  ⚠️  Server error {response.status_code}, retrying in {wait_time}s ({retry_count + 1}/{MAX_RETRIES})...")
                time.sleep(wait_time)
                return run_graphql_query(query, retry_count + 1)
            else:
                raise Exception(f"Server error {response.status_code} after {MAX_RETRIES} retries: {response.text}")
        
        # Handle authentication errors (don't retry)
        if response.status_code == 401:
            raise Exception("Authentication failed. Check your NOLOCO_API_TOKEN environment variable.")
        
        # Handle other HTTP errors
        if response.status_code != 200:
            raise Exception(f"API error: {response.status_code} - {response.text}")
        
        result = response.json()
        
        # Handle GraphQL errors
        if "errors" in result:
            error_messages = []
            for error in result["errors"]:
                msg = error.get("message", "Unknown error")
                error_messages.append(msg)
            raise Exception(f"GraphQL error: {'; '.join(error_messages)}")
        
        return result["data"]
        
    except requests.exceptions.Timeout:
        if retry_count < MAX_RETRIES:
            wait_time = RETRY_DELAY * (retry_count + 1)
            print(f"  ⚠️  Request timeout, retrying in {wait_time}s ({retry_count + 1}/{MAX_RETRIES})...")
            time.sleep(wait_time)
            return run_graphql_query(query, retry_count + 1)
        else:
            raise Exception(f"Request timeout after {MAX_RETRIES} retries")
    
    except requests.exceptions.ConnectionError as e:
        if retry_count < MAX_RETRIES:
            wait_time = RETRY_DELAY * (retry_count + 1)
            print(f"  ⚠️  Connection error, retrying in {wait_time}s ({retry_count + 1}/{MAX_RETRIES})...")
            time.sleep(wait_time)
            return run_graphql_query(query, retry_count + 1)
        else:
            raise Exception(f"Connection error after {MAX_RETRIES} retries. Check your internet connection.")


class NolocoPayrollAutomation:
    """
    Automates payroll record generation from approved timesheets in Noloco.
    Handles one-to-many relationship between payroll periods and timesheets.
    """
    
    def __init__(self):
        """Initialize with global configuration."""
        pass
    
    def get_all_timesheets(self) -> List[Dict]:
        """
        Download all timesheets from Noloco using GraphQL pagination.
        
        Returns:
            List of all timesheet records
        """
        print("📋 Fetching all timesheets...")
        
        all_records = []
        has_more_pages = True
        cursor = None
        page_number = 1
        
        try:
            while has_more_pages:
                if cursor:
                    query = f"""
                    query {{
                        timesheetsCollection(first: 100, after: "{cursor}") {{
                            edges {{
                                node {{
                                    id
                                    employeeIdVal
                                    approved
                                    payrollProcessed
                                    totalHours
                                    regularHours
                                    overtimeHours
                                    grossPay
                                    timesheetDate
                                    clockDatetime
                                    clockOutDatetime
                                }}
                            }}
                            pageInfo {{
                                hasNextPage
                                endCursor
                            }}
                        }}
                    }}
                    """
                else:
                    query = """
                    query {
                        timesheetsCollection(first: 100) {
                            edges {
                                node {
                                    id
                                    employeeIdVal
                                    approved
                                    payrollProcessed
                                    totalHours
                                    regularHours
                                    overtimeHours
                                    grossPay
                                    timesheetDate
                                    clockDatetime
                                    clockOutDatetime
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                        }
                    }
                    """
                
                data = run_graphql_query(query)
                collection = data.get("timesheetsCollection", {})
                edges = collection.get("edges", [])
                page_info = collection.get("pageInfo", {})
                
                for edge in edges:
                    node = edge.get("node", {})
                    all_records.append({
                        "id": node.get("id"),
                        "employee_id": node.get("employeeIdVal"),
                        "approved": node.get("approved"),
                        "payroll_processed": node.get("payrollProcessed"),
                        "total_hours": node.get("totalHours"),
                        "regular_hours": node.get("regularHours"),
                        "overtime_hours": node.get("overtimeHours"),
                        "gross_pay": node.get("grossPay"),
                        "timesheet_date": node.get("timesheetDate"),
                        "clock_in": node.get("clockDatetime"),
                        "clock_out": node.get("clockOutDatetime")
                    })
                
                print(f"  Downloaded page {page_number}: {len(edges)} records")
                
                has_more_pages = page_info.get("hasNextPage", False)
                cursor = page_info.get("endCursor")
                page_number += 1
            
            print(f"  ✓ Total timesheets: {len(all_records)}")
            return all_records
            
        except Exception as e:
            raise Exception(f"Failed to download timesheets: {str(e)}")
    
    def get_approved_timesheets(self) -> List[Dict]:
        """
        Get approved timesheets that haven't been processed for payroll.
        
        Returns:
            List of approved, unprocessed timesheet records
        """
        all_timesheets = self.get_all_timesheets()
        
        # Filter for approved and not processed
        approved_timesheets = [
            ts for ts in all_timesheets
            if ts.get('approved') == True and ts.get('payroll_processed') != True
        ]
        
        print(f"   Found {len(approved_timesheets)} approved, unprocessed timesheet(s)")
        return approved_timesheets
    
    def get_all_payroll(self) -> List[Dict]:
        """
        Download all payroll records from Noloco using GraphQL pagination.
        
        Returns:
            List of all payroll records
        """
        print("💰 Fetching all payroll records...")
        
        all_records = []
        has_more_pages = True
        cursor = None
        page_number = 1
        
        try:
            while has_more_pages:
                if cursor:
                    query = f"""
                    query {{
                        payrollCollection(first: 100, after: "{cursor}") {{
                            edges {{
                                node {{
                                    id
                                    employeeIdVal
                                    payPeriodStart
                                    payPeriodEnd
                                    totalHours
                                    regularHours
                                    overtimeHours
                                    grossPay
                                    status
                                }}
                            }}
                            pageInfo {{
                                hasNextPage
                                endCursor
                            }}
                        }}
                    }}
                    """
                else:
                    query = """
                    query {
                        payrollCollection(first: 100) {
                            edges {
                                node {
                                    id
                                    employeeIdVal
                                    payPeriodStart
                                    payPeriodEnd
                                    totalHours
                                    regularHours
                                    overtimeHours
                                    grossPay
                                    status
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                        }
                    }
                    """
                
                data = run_graphql_query(query)
                collection = data.get("payrollCollection", {})
                edges = collection.get("edges", [])
                page_info = collection.get("pageInfo", {})
                
                for edge in edges:
                    node = edge.get("node", {})
                    all_records.append({
                        "id": node.get("id"),
                        "employee_id": node.get("employeeIdVal"),
                        "pay_period_start": node.get("payPeriodStart"),
                        "pay_period_end": node.get("payPeriodEnd"),
                        "total_hours": node.get("totalHours"),
                        "regular_hours": node.get("regularHours"),
                        "overtime_hours": node.get("overtimeHours"),
                        "gross_pay": node.get("grossPay"),
                        "status": node.get("status")
                    })
                
                print(f"  Downloaded page {page_number}: {len(edges)} records")
                
                has_more_pages = page_info.get("hasNextPage", False)
                cursor = page_info.get("endCursor")
                page_number += 1
            
            print(f"  ✓ Total payroll records: {len(all_records)}")
            return all_records
            
        except Exception as e:
            raise Exception(f"Failed to download payroll records: {str(e)}")
    
    def get_payroll_records(self, employee_id: Optional[str] = None) -> List[Dict]:
        """
        Get payroll records, optionally filtered by employee.
        
        Args:
            employee_id: Optional employee ID to filter by
            
        Returns:
            List of payroll records
        """
        all_payroll = self.get_all_payroll()
        
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
            record_start = record.get('pay_period_start', '').split('T')[0]
            record_end = record.get('pay_period_end', '').split('T')[0]
            
            if (record_start == pay_period['start_date'] and
                record_end == pay_period['end_date']):
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
            total_hours += float(ts.get('total_hours', 0) or 0)
            total_regular_hours += float(ts.get('regular_hours', 0) or 0)
            total_overtime_hours += float(ts.get('overtime_hours', 0) or 0)
            total_gross_pay += float(ts.get('gross_pay', 0) or 0)
        
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
        
        # Format dates as ISO datetime strings with timezone
        period_start_dt = f"{pay_period['start_date']}T00:00:00-04:00"
        period_end_dt = f"{pay_period['end_date']}T23:59:59-04:00"
        created_dt = datetime.now().isoformat()
        
        mutation = f"""
        mutation {{
            createPayroll(
                employeeIdVal: "{employee_id}",
                payPeriodStart: "{period_start_dt}",
                payPeriodEnd: "{period_end_dt}",
                totalHours: {totals['total_hours']},
                regularHours: {totals['regular_hours']},
                overtimeHours: {totals['overtime_hours']},
                grossPay: {totals['gross_pay']},
                status: "pending",
                timesheetCount: {totals['timesheet_count']}
            ) {{
                id
            }}
        }}
        """
        
        result = run_graphql_query(mutation)
        payroll_id = result.get("createPayroll", {}).get("id")
        
        print(f"✅ Created payroll record for employee {employee_id}")
        print(f"   Pay Period: {pay_period['start_date']} to {pay_period['end_date']}")
        print(f"   Total Hours: {totals['total_hours']}, Gross Pay: ${totals['gross_pay']:.2f}")
        print(f"   Timesheets: {totals['timesheet_count']}")
        
        # Small delay to avoid rate limiting
        if RATE_LIMIT_DELAY > 0:
            time.sleep(RATE_LIMIT_DELAY)
        
        return {"id": payroll_id}
    
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
        
        # Calculate new totals
        new_totals = self.calculate_payroll_totals(new_timesheets)
        
        # Add to existing totals
        updated_total_hours = round(payroll_record.get('total_hours', 0) + new_totals['total_hours'], 2)
        updated_regular_hours = round(payroll_record.get('regular_hours', 0) + new_totals['regular_hours'], 2)
        updated_overtime_hours = round(payroll_record.get('overtime_hours', 0) + new_totals['overtime_hours'], 2)
        updated_gross_pay = round(payroll_record.get('gross_pay', 0) + new_totals['gross_pay'], 2)
        
        mutation = f"""
        mutation {{
            updatePayroll(
                id: "{payroll_id}",
                totalHours: {updated_total_hours},
                regularHours: {updated_regular_hours},
                overtimeHours: {updated_overtime_hours},
                grossPay: {updated_gross_pay}
            ) {{
                id
            }}
        }}
        """
        
        result = run_graphql_query(mutation)
        
        print(f"✅ Updated payroll record {payroll_id}")
        print(f"   Added {len(new_timesheets)} new timesheet(s)")
        print(f"   New Total Hours: {updated_total_hours}, Gross Pay: ${updated_gross_pay:.2f}")
        
        # Small delay to avoid rate limiting
        if RATE_LIMIT_DELAY > 0:
            time.sleep(RATE_LIMIT_DELAY)
        
        return result
    
    def mark_timesheets_processed(self, timesheet_ids: List[str]):
        """
        Mark timesheets as processed for payroll.
        
        Args:
            timesheet_ids: List of timesheet IDs to mark
        """
        for ts_id in timesheet_ids:
            try:
                mutation = f"""
                mutation {{
                    updateTimesheets(
                        id: "{ts_id}",
                        payrollProcessed: true
                    ) {{
                        id
                    }}
                }}
                """
                
                run_graphql_query(mutation)
                print(f"   ✓ Marked timesheet {ts_id} as processed")
                
                # Small delay to avoid rate limiting
                if RATE_LIMIT_DELAY > 0:
                    time.sleep(RATE_LIMIT_DELAY)
                    
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
            timesheet_date = ts.get('timesheet_date') or ts.get('clock_in')
            
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
    print("=" * 70)
    print("Pet Esthetic Payroll Processing")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # Initialize automation
        automation = NolocoPayrollAutomation()
        
        # Run the process
        automation.process_timesheets_to_payroll()
        
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("\nTroubleshooting tips:")
        print("- Check your NOLOCO_API_TOKEN and NOLOCO_PROJECT_ID environment variables")
        print("- Verify field names match your Noloco schema")
        print("- Check if table structures have changed in Noloco")
        exit(1)


if __name__ == "__main__":
    main()

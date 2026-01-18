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
    
    def get_employee_pay_rate(self, employee_id: str) -> float:
        """
        Get the pay rate for an employee from the Employees table.
        
        Args:
            employee_id: Employee ID value
            
        Returns:
            Pay rate as float, or 0.0 if not found
        """
        try:
            query = f"""
            query {{
                employeesCollection(filter: {{employeeId: "{employee_id}"}}) {{
                    edges {{
                        node {{
                            id
                            payRate
                        }}
                    }}
                }}
            }}
            """
            
            data = run_graphql_query(query)
            collection = data.get("employeesCollection", {})
            edges = collection.get("edges", [])
            
            if edges and len(edges) > 0:
                pay_rate = edges[0].get("node", {}).get("payRate", 0.0)
                return float(pay_rate) if pay_rate else 0.0
            
            print(f"  ⚠️  Warning: No pay rate found for employee {employee_id}")
            return 0.0
            
        except Exception as e:
            print(f"  ⚠️  Warning: Could not fetch pay rate for employee {employee_id}: {e}")
            return 0.0
    
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
                                    employeeId
                                    approved
                                    payrollProcessed
                                    periodStartDate
                                    periodEndDate
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
                                    employeeId
                                    approved
                                    payrollProcessed
                                    periodStartDate
                                    periodEndDate
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
                        "employee_id": node.get("employeeId"),
                        "approved": node.get("approved"),
                        "payroll_processed": node.get("payrollProcessed"),
                        "period_start_date": node.get("periodStartDate"),
                        "period_end_date": node.get("periodEndDate")
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
                                    employeeId
                                    payPeriodStart
                                    payPeriodEnd
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
                                    employeeId
                                    payPeriodStart
                                    payPeriodEnd
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
                        "employee_id": node.get("employeeId"),
                        "pay_period_start": node.get("payPeriodStart"),
                        "pay_period_end": node.get("payPeriodEnd"),
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
    
    def get_payroll_records(self, employee_id: Optional[str] = None, 
                           pay_period: Optional[Dict[str, str]] = None) -> List[Dict]:
        """
        Get payroll records, optionally filtered by employee and/or pay period.
        
        Args:
            employee_id: Optional employee ID to filter by
            pay_period: Optional dict with 'start_date' and 'end_date'
            
        Returns:
            List of payroll records
        """
        all_payroll = self.get_all_payroll()
        
        if employee_id:
            all_payroll = [p for p in all_payroll if p.get('employee_id') == employee_id]
        
        if pay_period:
            filtered_payroll = []
            for p in all_payroll:
                p_start = p.get('pay_period_start', '').split('T')[0]
                p_end = p.get('pay_period_end', '').split('T')[0]
                
                if (p_start == pay_period['start_date'] and 
                    p_end == pay_period['end_date']):
                    filtered_payroll.append(p)
            
            all_payroll = filtered_payroll
        
        return all_payroll
    
    def find_existing_payroll(self, employee_id: str, pay_period: Dict[str, str]) -> Optional[Dict]:
        """
        Find existing payroll record for employee and pay period.
        
        Args:
            employee_id: Employee ID
            pay_period: Dict with start_date and end_date
            
        Returns:
            Existing payroll record or None
        """
        payroll_records = self.get_payroll_records(employee_id, pay_period)
        
        if payroll_records and len(payroll_records) > 0:
            return payroll_records[0]
        
        return None
    
    def create_payroll_record(self, employee_id: str, timesheets: List[Dict], 
                             pay_period: Dict[str, str]) -> Dict:
        """
        Create new payroll record from timesheets.
        
        Args:
            employee_id: Employee ID
            timesheets: List of approved timesheets
            pay_period: Pay period dates from the timesheets
            
        Returns:
            Created payroll record
        """
        # Get pay rate from employee record
        pay_rate = self.get_employee_pay_rate(employee_id)
        
        # Get timesheet IDs for the relationship
        timesheet_ids = [ts.get('id') for ts in timesheets]
        
        # Format dates as ISO datetime strings with timezone
        period_start_dt = f"{pay_period['start_date']}T00:00:00-04:00"
        period_end_dt = f"{pay_period['end_date']}T23:59:59-04:00"
        
        # Build the mutation with relationship IDs
        # Note: relatedTimesheetsId expects an array of IDs
        timesheet_ids_str = ', '.join([f'"{tid}"' for tid in timesheet_ids])
        
        mutation = f"""
        mutation {{
            createPayroll(
                employeeId: "{employee_id}",
                payPeriodStart: "{period_start_dt}",
                payPeriodEnd: "{period_end_dt}",
                payRate: {pay_rate},
                paymentMethod: "Direct Deposit",
                status: "Pending",
                relatedTimesheetsId: [{timesheet_ids_str}]
            ) {{
                id
            }}
        }}
        """
        
        result = run_graphql_query(mutation)
        payroll_id = result.get("createPayroll", {}).get("id")
        
        print(f"✅ Created payroll record for employee {employee_id}")
        print(f"   Pay Period: {pay_period['start_date']} to {pay_period['end_date']}")
        print(f"   Pay Rate: ${pay_rate:.2f}/hr")
        print(f"   Timesheets: {len(timesheets)}")
        
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
        
        # Get new timesheet IDs
        new_timesheet_ids = [ts.get('id') for ts in new_timesheets]
        
        # Build array of IDs for the relationship update
        timesheet_ids_str = ', '.join([f'"{tid}"' for tid in new_timesheet_ids])
        
        # Note: This will ADD to the existing relationship
        mutation = f"""
        mutation {{
            updatePayroll(
                id: "{payroll_id}",
                relatedTimesheetsId: [{timesheet_ids_str}]
            ) {{
                id
            }}
        }}
        """
        
        result = run_graphql_query(mutation)
        
        print(f"✅ Updated payroll record {payroll_id}")
        print(f"   Added {len(new_timesheets)} new timesheet(s)")
        
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
        # Using the period dates from the timesheet itself
        grouped_timesheets = {}
        
        for ts in approved_timesheets:
            employee_id = ts.get('employee_id')
            period_start = ts.get('period_start_date')
            period_end = ts.get('period_end_date')
            
            if not employee_id:
                print(f"⚠️  Skipping timesheet {ts.get('id')} - missing employee_id")
                continue
            
            if not period_start or not period_end:
                print(f"⚠️  Skipping timesheet {ts.get('id')} - missing period dates")
                continue
            
            # Extract just the date part (remove time if present)
            period_start_date = period_start.split('T')[0]
            period_end_date = period_end.split('T')[0]
            
            pay_period = {
                'start_date': period_start_date,
                'end_date': period_end_date
            }
            
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

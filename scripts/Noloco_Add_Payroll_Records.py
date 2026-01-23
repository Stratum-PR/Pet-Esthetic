import requests
import os
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
import json
import time
from zoneinfo import ZoneInfo

# Try to load dotenv, but make it optional
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # If dotenv not available, environment variables must be set manually
    pass

# ============================================================================
# CONFIGURATION - Update these with your credentials
# ============================================================================
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

# Timezone configuration
PR_TIMEZONE = ZoneInfo('America/Puerto_Rico')

# Default values (can be overridden via environment variables)
DEFAULT_PAYMENT_METHOD = os.getenv('DEFAULT_PAYMENT_METHOD', 'Direct Deposit')
DEFAULT_PAYROLL_STATUS = os.getenv('DEFAULT_PAYROLL_STATUS', 'Pending')
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
        # Disable proxy for Noloco API requests
        # Some systems have misconfigured proxy settings that interfere
        proxies = {
            'http': None,
            'https': None
        }
        
        response = requests.post(
            API_URL,
            headers=HEADERS,
            json={"query": query},
            proxies=proxies,
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


def calculate_biweekly_pay_period(target_date: datetime) -> Dict[str, str]:
    """
    Calculate bi-weekly pay period containing target_date.
    Pay periods: Monday to Sunday, 14 days.
    Reference: Jan 12, 2026 (Monday) is first Monday of cycle.
    
    Args:
        target_date: Datetime object (timezone-aware or naive)
        
    Returns:
        Dict with 'start_date' and 'end_date' (YYYY-MM-DD format)
    """
    # Ensure target_date is timezone-aware (convert to PR timezone if naive)
    if target_date.tzinfo is None:
        target_date = target_date.replace(tzinfo=PR_TIMEZONE)
    else:
        target_date = target_date.astimezone(PR_TIMEZONE)
    
    # Reference Monday: Jan 12, 2026
    reference_monday = datetime(2026, 1, 12, tzinfo=PR_TIMEZONE)
    
    # Calculate days difference
    target_date_only = target_date.date()
    reference_date_only = reference_monday.date()
    days_diff = (target_date_only - reference_date_only).days
    
    # Find which bi-weekly cycle (0 or 1, or negative cycles)
    # Each cycle is 14 days
    cycle_number = days_diff // 14
    cycle_offset = cycle_number * 14
    
    # Calculate Monday of current cycle
    period_monday = reference_monday + timedelta(days=cycle_offset)
    
    # Calculate Sunday (13 days later, so total period is 14 days: Mon-Sun)
    period_sunday = period_monday + timedelta(days=13)
    
    return {
        'start_date': period_monday.strftime('%Y-%m-%d'),
        'end_date': period_sunday.strftime('%Y-%m-%d')
    }


def calculate_payment_date(pay_period_end: str) -> str:
    """
    Calculate payment date (next Monday after pay period ends).
    
    Args:
        pay_period_end: End date string (YYYY-MM-DD format, should be Sunday)
        
    Returns:
        Payment date string (YYYY-MM-DD format, Monday)
    """
    end_date = datetime.strptime(pay_period_end, '%Y-%m-%d').date()
    # Find next Monday (0 = Monday)
    # If end_date is Sunday (6), days_until_monday = 1
    # If end_date is Monday (0), days_until_monday = 7 (shouldn't happen for Sunday end)
    days_until_monday = (7 - end_date.weekday()) % 7
    if days_until_monday == 0:  # Already Monday (shouldn't happen for Sunday end)
        days_until_monday = 7
    payment_date = end_date + timedelta(days=days_until_monday)
    return payment_date.strftime('%Y-%m-%d')


def get_current_pay_period() -> Dict[str, str]:
    """
    Get the current bi-weekly pay period for today.
    
    Returns:
        Dict with 'start_date' and 'end_date' (YYYY-MM-DD format)
    """
    now = datetime.now(PR_TIMEZONE)
    return calculate_biweekly_pay_period(now)


def calculate_total_hours(timesheets: List[Dict]) -> float:
    """
    Sum shiftHoursWorked from all timesheets.
    
    Args:
        timesheets: List of timesheet dictionaries
        
    Returns:
        Total hours as float
    """
    total = 0.0
    for ts in timesheets:
        hours = ts.get('shift_hours_worked')
        if hours is not None:
            try:
                total += float(hours)
            except (ValueError, TypeError):
                pass
    return total


def validate_timesheet(ts: Dict) -> tuple[bool, Optional[str]]:
    """
    Validate timesheet record has required fields.
    
    Args:
        ts: Timesheet dictionary
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not ts.get('id'):
        return False, "Missing timesheet ID"
    
    if not ts.get('employee_id'):
        return False, "Missing employee_id"
    
    if ts.get('timesheet_date') is None:
        return False, "Missing timesheet_date"
    
    if ts.get('employee_pin') is None:
        return False, "Missing employee_pin"
    
    # Validate shiftHoursWorked is numeric if present
    hours = ts.get('shift_hours_worked')
    if hours is not None:
        try:
            float(hours)
        except (ValueError, TypeError):
            return False, f"Invalid shiftHoursWorked value: {hours}"
    
    return True, None


def validate_pay_period(pay_period: Dict[str, str]) -> Tuple[bool, Optional[str]]:
    """
    Validate pay period dates.
    
    Args:
        pay_period: Dict with 'start_date' and 'end_date'
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not pay_period.get('start_date'):
        return False, "Missing start_date"
    
    if not pay_period.get('end_date'):
        return False, "Missing end_date"
    
    try:
        start_date = datetime.strptime(pay_period['start_date'], '%Y-%m-%d').date()
        end_date = datetime.strptime(pay_period['end_date'], '%Y-%m-%d').date()
        
        if start_date > end_date:
            return False, "start_date must be before or equal to end_date"
        
    except ValueError as e:
        return False, f"Invalid date format: {e}"
    
    return True, None


def normalize_datetime_for_comparison(dt_string: Optional[str]) -> Optional[str]:
    """
    Normalize datetime string for comparison (removes timezone, milliseconds).
    
    Args:
        dt_string: Datetime string in various formats
        
    Returns:
        Normalized datetime string or None
    """
    if not dt_string:
        return None
    
    try:
        dt_string = str(dt_string).strip()
        
        # Remove timezone and milliseconds
        if 'T' in dt_string:
            # ISO format
            clean_string = dt_string.split('+')[0].split('Z')[0].split('.')[0]
            dt = datetime.fromisoformat(clean_string)
        else:
            # Date-only or other format
            clean_string = dt_string.split(' ')[0]
            dt = datetime.strptime(clean_string, '%Y-%m-%d')
        
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, AttributeError):
        return None


def validate_no_duplicate_payroll_per_employee(
    grouped_timesheets: Dict,
    existing_payroll_records: List[Dict]
) -> Tuple[bool, List[str]]:
    """
    CRITICAL: Validate no duplicate payroll records per employee and pay period.
    
    Args:
        grouped_timesheets: Dict of grouped timesheets by employee/pay period
        existing_payroll_records: List of existing payroll records
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # Check for duplicates in existing payroll records
    employee_period_map = {}
    for payroll in existing_payroll_records:
        emp_id = payroll.get('employee_id')
        period_start = payroll.get('pay_period_start', '').split('T')[0]
        period_end = payroll.get('pay_period_end', '').split('T')[0]
        
        if emp_id and period_start and period_end:
            key = f"{emp_id}_{period_start}_{period_end}"
            if key in employee_period_map:
                errors.append(
                    f"CRITICAL: Duplicate payroll record found for employee {emp_id} "
                    f"and pay period {period_start} to {period_end}. "
                    f"Existing IDs: {employee_period_map[key]}, {payroll.get('id')}"
                )
            else:
                employee_period_map[key] = payroll.get('id')
    
    # Check for duplicates in what we're about to create
    new_keys = {}
    for key, group in grouped_timesheets.items():
        emp_id = group.get('employee_id')
        pay_period = group.get('pay_period', {})
        period_start = pay_period.get('start_date')
        period_end = pay_period.get('end_date')
        
        if emp_id and period_start and period_end:
            check_key = f"{emp_id}_{period_start}_{period_end}"
            
            # Check against existing
            if check_key in employee_period_map:
                errors.append(
                    f"CRITICAL: Payroll record already exists for employee {emp_id} "
                    f"and pay period {period_start} to {period_end}. "
                    f"Existing payroll ID: {employee_period_map[check_key]}"
                )
            
            # Check for duplicates in new records
            if check_key in new_keys:
                errors.append(
                    f"CRITICAL: Multiple payroll records would be created for employee {emp_id} "
                    f"and pay period {period_start} to {period_end}"
                )
            else:
                new_keys[check_key] = key
    
    return len(errors) == 0, errors


def validate_same_pay_period_for_all(
    grouped_timesheets: Dict
) -> Tuple[bool, Optional[str]]:
    """
    CRITICAL: Validate all employees have the same pay period.
    
    Args:
        grouped_timesheets: Dict of grouped timesheets by employee/pay period
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not grouped_timesheets:
        return True, None
    
    # Get first pay period as reference
    first_group = list(grouped_timesheets.values())[0]
    reference_period = first_group.get('pay_period', {})
    ref_start = reference_period.get('start_date')
    ref_end = reference_period.get('end_date')
    
    # Check all other groups have same period
    for key, group in grouped_timesheets.items():
        pay_period = group.get('pay_period', {})
        period_start = pay_period.get('start_date')
        period_end = pay_period.get('end_date')
        
        if period_start != ref_start or period_end != ref_end:
            emp_id = group.get('employee_id')
            return False, (
                f"CRITICAL: Pay period mismatch! Employee {emp_id} has period "
                f"{period_start} to {period_end}, but expected {ref_start} to {ref_end}"
            )
    
    return True, None


def validate_no_duplicate_clock_times(
    timesheets: List[Dict]
) -> Tuple[bool, List[str]]:
    """
    CRITICAL: Validate no timesheets have duplicate clock in/out datetimes.
    
    Args:
        timesheets: List of timesheet dictionaries
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    clock_time_pairs = {}
    
    for ts in timesheets:
        ts_id = ts.get('id', 'unknown')
        clock_in = ts.get('clock_datetime')
        clock_out = ts.get('clock_out_datetime')
        
        if not clock_in or not clock_out:
            continue
        
        # Normalize for comparison
        clock_in_norm = normalize_datetime_for_comparison(clock_in)
        clock_out_norm = normalize_datetime_for_comparison(clock_out)
        
        if not clock_in_norm or not clock_out_norm:
            continue
        
        # Create unique key for clock in/out pair
        clock_pair_key = f"{clock_in_norm}|{clock_out_norm}"
        
        if clock_pair_key in clock_time_pairs:
            existing_ts_id = clock_time_pairs[clock_pair_key]
            errors.append(
                f"CRITICAL: Duplicate clock times found! "
                f"Timesheet {ts_id} has same clock in/out as timesheet {existing_ts_id}. "
                f"Clock In: {clock_in_norm}, Clock Out: {clock_out_norm}"
            )
        else:
            clock_time_pairs[clock_pair_key] = ts_id
    
    return len(errors) == 0, errors


def validate_pre_upload(
    grouped_timesheets: Dict,
    existing_payroll_records: List[Dict]
) -> Tuple[bool, List[str]]:
    """
    CRITICAL: Comprehensive pre-upload validation.
    Prevents financial errors before creating payroll records.
    
    Args:
        grouped_timesheets: Dict of grouped timesheets by employee/pay period
        existing_payroll_records: List of existing payroll records
        
    Returns:
        Tuple of (is_valid, list_of_all_errors)
    """
    all_errors = []
    
    # Validation 1: No duplicate payroll per employee/pay period
    is_valid, errors = validate_no_duplicate_payroll_per_employee(
        grouped_timesheets, existing_payroll_records
    )
    if not is_valid:
        all_errors.extend(errors)
    
    # Validation 2: Same pay period for all employees
    is_valid, error = validate_same_pay_period_for_all(grouped_timesheets)
    if not is_valid:
        all_errors.append(error)
    
    # Validation 3: No duplicate clock times in timesheets
    for key, group in grouped_timesheets.items():
        timesheets = group.get('timesheets', [])
        is_valid, errors = validate_no_duplicate_clock_times(timesheets)
        if not is_valid:
            all_errors.extend(errors)
    
    return len(all_errors) == 0, all_errors


def verify_post_upload(
    payroll_id: str,
    expected_employee_id: str,
    expected_pay_period: Dict[str, str],
    expected_timesheet_ids: List[str]
) -> Tuple[bool, List[str]]:
    """
    CRITICAL: Post-upload verification to ensure payroll was created correctly.
    
    Args:
        payroll_id: ID of created/updated payroll record
        expected_employee_id: Expected employee ID
        expected_pay_period: Expected pay period dict
        expected_timesheet_ids: Expected list of timesheet IDs
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    try:
        # Fetch the payroll record we just created
        query = f"""
        query {{
            payrollCollection(filter: {{id: "{payroll_id}"}}) {{
                edges {{
                    node {{
                        id
                        employeeId
                        payPeriodStart
                        payPeriodEnd
                        relatedTimesheets {{
                            edges {{
                                node {{
                                    id
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
        """
        
        data = run_graphql_query(query)
        collection = data.get("payrollCollection", {})
        edges = collection.get("edges", [])
        
        if not edges:
            errors.append(f"CRITICAL: Payroll record {payroll_id} not found after creation!")
            return False, errors
        
        node = edges[0].get("node", {})
        
        # Verify employee ID
        actual_employee_id = node.get("employeeId")
        if actual_employee_id != expected_employee_id:
            errors.append(
                f"CRITICAL: Employee ID mismatch! Expected {expected_employee_id}, "
                f"got {actual_employee_id}"
            )
        
        # Verify pay period
        period_start = node.get("payPeriodStart", "").split("T")[0]
        period_end = node.get("payPeriodEnd", "").split("T")[0]
        
        if period_start != expected_pay_period.get('start_date'):
            errors.append(
                f"CRITICAL: Pay period start mismatch! Expected {expected_pay_period.get('start_date')}, "
                f"got {period_start}"
            )
        
        if period_end != expected_pay_period.get('end_date'):
            errors.append(
                f"CRITICAL: Pay period end mismatch! Expected {expected_pay_period.get('end_date')}, "
                f"got {period_end}"
            )
        
        # Verify related timesheets
        related_timesheets = node.get("relatedTimesheets", {})
        timesheet_edges = related_timesheets.get("edges", [])
        actual_timesheet_ids = [edge.get("node", {}).get("id") for edge in timesheet_edges]
        
        expected_set = set(expected_timesheet_ids)
        actual_set = set(actual_timesheet_ids)
        
        missing = expected_set - actual_set
        extra = actual_set - expected_set
        
        if missing:
            errors.append(
                f"CRITICAL: Missing timesheets in payroll! Expected {list(missing)}, "
                f"but not found in related timesheets"
            )
        
        if extra:
            errors.append(
                f"WARNING: Extra timesheets in payroll! Found {list(extra)}, "
                f"but not expected"
            )
        
    except Exception as e:
        errors.append(f"CRITICAL: Error verifying payroll record: {e}")
    
    return len(errors) == 0, errors


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
    
    def get_all_timesheets(self, filter_approved: bool = False) -> List[Dict]:
        """
        Download all timesheets from Noloco using GraphQL pagination.
        
        Args:
            filter_approved: If True, only fetch approved and unprocessed timesheets
        
        Returns:
            List of all timesheet records
        """
        print("Fetching all timesheets...")
        
        all_records = []
        has_more_pages = True
        cursor = None
        page_number = 1
        
        try:
            while has_more_pages:
                if cursor:
                    if filter_approved:
                        query = f"""
                        query {{
                            timesheetsCollection(first: 100, after: "{cursor}", filter: {{approved: true, payrollProcessed: false}}) {{
                                edges {{
                                    node {{
                                        id
                                        employeeId
                                        approved
                                        payrollProcessed
                                    timesheetDate
                                    employeePin
                                    shiftHoursWorked
                                    clockDatetime
                                    clockOutDatetime
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
                        query = f"""
                        query {{
                            timesheetsCollection(first: 100, after: "{cursor}") {{
                                edges {{
                                    node {{
                                        id
                                        employeeId
                                        approved
                                        payrollProcessed
                                    timesheetDate
                                    employeePin
                                    shiftHoursWorked
                                    clockDatetime
                                    clockOutDatetime
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
                    if filter_approved:
                        query = """
                        query {
                            timesheetsCollection(first: 100, filter: {approved: true, payrollProcessed: false}) {
                                edges {
                                    node {
                                        id
                                        employeeId
                                        approved
                                        payrollProcessed
                                    timesheetDate
                                    employeePin
                                    shiftHoursWorked
                                    clockDatetime
                                    clockOutDatetime
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
                                    timesheetDate
                                    employeePin
                                    shiftHoursWorked
                                    clockDatetime
                                    clockOutDatetime
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
                        "timesheet_date": node.get("timesheetDate"),
                        "employee_pin": node.get("employeePin"),
                        "shift_hours_worked": node.get("shiftHoursWorked"),
                        "clock_datetime": node.get("clockDatetime"),
                        "clock_out_datetime": node.get("clockOutDatetime"),
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
    
    def get_approved_timesheets(self, pay_period: Optional[Dict[str, str]] = None) -> List[Dict]:
        """
        Get approved timesheets that haven't been processed for payroll.
        Optionally filter by pay period (timesheetDate must fall within period).
        
        Args:
            pay_period: Optional dict with 'start_date' and 'end_date' (YYYY-MM-DD)
        
        Returns:
            List of approved, unprocessed timesheet records
        """
        # Use GraphQL filter for approved/unprocessed (more efficient)
        all_timesheets = self.get_all_timesheets(filter_approved=True)
        
        # Filter by pay period if provided
        if pay_period:
            period_start = pay_period.get('start_date')
            period_end = pay_period.get('end_date')
            
            if period_start and period_end:
                filtered_timesheets = []
                for ts in approved_timesheets:
                    timesheet_date_str = ts.get('timesheet_date')
                    if not timesheet_date_str:
                        continue
                    
                    # Parse timesheet date (handle various formats)
                    try:
                        # Try ISO format first
                        if 'T' in timesheet_date_str:
                            ts_date = datetime.fromisoformat(timesheet_date_str.split('+')[0].split('Z')[0]).date()
                        else:
                            # Try date-only format
                            ts_date = datetime.strptime(timesheet_date_str.split(' ')[0], '%Y-%m-%d').date()
                        
                        period_start_date = datetime.strptime(period_start, '%Y-%m-%d').date()
                        period_end_date = datetime.strptime(period_end, '%Y-%m-%d').date()
                        
                        # Check if timesheet date falls within pay period
                        if period_start_date <= ts_date <= period_end_date:
                            filtered_timesheets.append(ts)
                    except (ValueError, AttributeError) as e:
                        print(f"  ⚠️  Warning: Could not parse timesheet date '{timesheet_date_str}': {e}")
                        continue
                
                approved_timesheets = filtered_timesheets
                print(f"   Filtered to {len(approved_timesheets)} timesheet(s) within pay period {period_start} to {period_end}")
        
        print(f"   Found {len(approved_timesheets)} approved, unprocessed timesheet(s)")
        return approved_timesheets
    
    def get_all_payroll(self, employee_id: Optional[str] = None) -> List[Dict]:
        """
        Download all payroll records from Noloco using GraphQL pagination.
        
        Args:
            employee_id: Optional employee ID to filter by
        
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
                    if employee_id:
                        query = f"""
                        query {{
                            payrollCollection(first: 100, after: "{cursor}", filter: {{employeeId: "{employee_id}"}}) {{
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
                    if employee_id:
                        query = f"""
                        query {{
                            payrollCollection(first: 100, filter: {{employeeId: "{employee_id}"}}) {{
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
        # Use GraphQL filter for employee_id (more efficient)
        all_payroll = self.get_all_payroll(employee_id=employee_id)
        
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
        Also fetches related timesheets to check for duplicates.
        
        Args:
            employee_id: Employee ID
            pay_period: Dict with start_date and end_date
            
        Returns:
            Existing payroll record with related_timesheet_ids, or None
        """
        payroll_records = self.get_payroll_records(employee_id, pay_period)
        
        if payroll_records and len(payroll_records) > 0:
            payroll_record = payroll_records[0]
            payroll_id = payroll_record.get('id')
            
            # Fetch related timesheets
            try:
                query = f"""
                query {{
                    payrollCollection(filter: {{id: "{payroll_id}"}}) {{
                        edges {{
                            node {{
                                id
                                relatedTimesheets {{
                                    edges {{
                                        node {{
                                            id
                                            shiftHoursWorked
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
                """
                
                data = run_graphql_query(query)
                collection = data.get("payrollCollection", {})
                edges = collection.get("edges", [])
                
                if edges:
                    node = edges[0].get("node", {})
                    related_timesheets = node.get("relatedTimesheets", {})
                    timesheet_edges = related_timesheets.get("edges", [])
                    
                    existing_timesheet_ids = []
                    existing_hours = 0.0
                    
                    for ts_edge in timesheet_edges:
                        ts_node = ts_edge.get("node", {})
                        ts_id = ts_node.get("id")
                        hours = ts_node.get("shiftHoursWorked")
                        
                        if ts_id:
                            existing_timesheet_ids.append(ts_id)
                        if hours:
                            try:
                                existing_hours += float(hours)
                            except (ValueError, TypeError):
                                pass
                    
                    payroll_record['related_timesheet_ids'] = existing_timesheet_ids
                    payroll_record['existing_hours'] = existing_hours
                    
            except Exception as e:
                print(f"  ⚠️  Warning: Could not fetch related timesheets: {e}")
                payroll_record['related_timesheet_ids'] = []
                payroll_record['existing_hours'] = 0.0
            
            return payroll_record
        
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
        
        # Get employee PIN from first timesheet (all should have same employee)
        employee_pin = timesheets[0].get('employee_pin') if timesheets else None
        if not employee_pin:
            print(f"  ⚠️  Warning: No employeePin found in timesheets for employee {employee_id}")
        
        # Calculate total hours worked
        total_hours = calculate_total_hours(timesheets)
        
        # Calculate payment date (next Monday after period ends)
        payment_date = calculate_payment_date(pay_period['end_date'])
        
        # Format dates as ISO datetime strings with timezone
        period_start_dt = datetime.strptime(pay_period['start_date'], '%Y-%m-%d')
        period_start_dt = period_start_dt.replace(tzinfo=PR_TIMEZONE)
        period_end_dt = datetime.strptime(pay_period['end_date'], '%Y-%m-%d')
        period_end_dt = period_end_dt.replace(hour=23, minute=59, second=59, tzinfo=PR_TIMEZONE)
        
        # Format payment date
        payment_date_dt = datetime.strptime(payment_date, '%Y-%m-%d')
        payment_date_dt = payment_date_dt.replace(tzinfo=PR_TIMEZONE)
        
        # Use isoformat() which properly formats timezone as -04:00
        period_start_iso = period_start_dt.isoformat()
        period_end_iso = period_end_dt.isoformat()
        payment_date_iso = payment_date_dt.isoformat()
        
        # Build the mutation with relationship IDs
        # Note: relatedTimesheetsId expects an array of IDs
        timesheet_ids_str = ', '.join([f'"{tid}"' for tid in timesheet_ids])
        
        # Build mutation with all fields
        employee_pin_str = f'"{employee_pin}"' if employee_pin else 'null'
        
        mutation = f"""
        mutation {{
            createPayroll(
                employeeId: "{employee_id}",
                payPeriodStart: "{period_start_iso}",
                payPeriodEnd: "{period_end_iso}",
                payRate: {pay_rate},
                paymentMethod: "{DEFAULT_PAYMENT_METHOD}",
                status: "{DEFAULT_PAYROLL_STATUS}",
                paymentDate2: "{payment_date_iso}",
                employeeIdVal: {employee_pin_str},
                totalHoursWorked2: {total_hours},
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
        print(f"   Payment Date: {payment_date}")
        print(f"   Pay Rate: ${pay_rate:.2f}/hr")
        print(f"   Total Hours: {total_hours:.2f}")
        print(f"   Employee PIN: {employee_pin}")
        print(f"   Timesheets: {len(timesheets)}")
        
        # Small delay to avoid rate limiting
        if RATE_LIMIT_DELAY > 0:
            time.sleep(RATE_LIMIT_DELAY)
        
        return {"id": payroll_id}
    
    def update_payroll_record(self, payroll_record: Dict, new_timesheets: List[Dict]) -> Dict:
        """
        Update existing payroll record with additional timesheets.
        Only adds timesheets that aren't already linked.
        Recalculates totalHoursWorked2.
        
        Args:
            payroll_record: Existing payroll record (with related_timesheet_ids)
            new_timesheets: New timesheets to add to this payroll
            
        Returns:
            Updated payroll record
        """
        payroll_id = payroll_record.get('id')
        
        # Get existing timesheet IDs
        existing_timesheet_ids = set(payroll_record.get('related_timesheet_ids', []))
        
        # Get new timesheet IDs and filter out duplicates
        new_timesheet_ids = []
        truly_new_timesheets = []
        
        for ts in new_timesheets:
            ts_id = ts.get('id')
            if ts_id and ts_id not in existing_timesheet_ids:
                new_timesheet_ids.append(ts_id)
                truly_new_timesheets.append(ts)
            elif ts_id in existing_timesheet_ids:
                print(f"   ⚠️  Timesheet {ts_id} already linked, skipping")
        
        if not new_timesheet_ids:
            print(f"✅ No new timesheets to add to payroll record {payroll_id}")
            return {"id": payroll_id}
        
        # Calculate new hours from new timesheets
        new_hours = calculate_total_hours(truly_new_timesheets)
        existing_hours = payroll_record.get('existing_hours', 0.0)
        total_hours = existing_hours + new_hours
        
        # Merge all timesheet IDs (existing + new)
        all_timesheet_ids = list(existing_timesheet_ids) + new_timesheet_ids
        timesheet_ids_str = ', '.join([f'"{tid}"' for tid in all_timesheet_ids])
        
        # Update payroll with merged timesheet IDs and new total hours
        mutation = f"""
        mutation {{
            updatePayroll(
                id: "{payroll_id}",
                relatedTimesheetsId: [{timesheet_ids_str}],
                totalHoursWorked2: {total_hours}
            ) {{
                id
            }}
        }}
        """
        
        result = run_graphql_query(mutation)
        
        print(f"✅ Updated payroll record {payroll_id}")
        print(f"   Added {len(new_timesheet_ids)} new timesheet(s)")
        print(f"   {len(existing_timesheet_ids)} timesheet(s) already existed")
        print(f"   Total Hours: {total_hours:.2f} (existing: {existing_hours:.2f}, new: {new_hours:.2f})")
        
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
        Uses current bi-weekly pay period for filtering.
        """
        print("\n" + "="*70)
        print("🚀 STARTING PAYROLL PROCESSING")
        print("="*70 + "\n")
        
        # Calculate current pay period
        current_pay_period = get_current_pay_period()
        print(f"📅 Current Pay Period: {current_pay_period['start_date']} to {current_pay_period['end_date']}\n")
        
        # Get approved timesheets for current pay period
        approved_timesheets = self.get_approved_timesheets(pay_period=current_pay_period)
        
        if not approved_timesheets:
            print("✓ No approved timesheets to process for current pay period.\n")
            return
        
        print(f"📊 Processing {len(approved_timesheets)} approved timesheet(s)\n")
        
        # Group timesheets by employee
        # All timesheets are already in the same pay period (current period)
        grouped_timesheets = {}
        
        for ts in approved_timesheets:
            # Validate timesheet
            is_valid, error_msg = validate_timesheet(ts)
            if not is_valid:
                print(f"⚠️  Skipping timesheet {ts.get('id', 'unknown')} - {error_msg}")
                continue
            
            employee_id = ts.get('employee_id')
            
            # Use current pay period for all timesheets
            key = f"{employee_id}_{current_pay_period['start_date']}_{current_pay_period['end_date']}"
            
            if key not in grouped_timesheets:
                grouped_timesheets[key] = {
                    'employee_id': employee_id,
                    'pay_period': current_pay_period,
                    'timesheets': []
                }
            
            grouped_timesheets[key]['timesheets'].append(ts)
        
        # CRITICAL: Pre-upload validation
        print("\n" + "="*70)
        print("🔍 PRE-UPLOAD VALIDATION")
        print("="*70)
        
        # Get all existing payroll records for validation
        all_existing_payroll = self.get_all_payroll()
        
        is_valid, validation_errors = validate_pre_upload(grouped_timesheets, all_existing_payroll)
        
        if not is_valid:
            print("\n❌ CRITICAL VALIDATION ERRORS DETECTED - ABORTING PROCESSING")
            print("="*70)
            for error in validation_errors:
                print(f"  ❌ {error}")
            print("="*70)
            print("\n⚠️  No payroll records were created. Please fix the errors above and try again.")
            raise Exception("Pre-upload validation failed. See errors above.")
        
        print("✅ All pre-upload validations passed")
        print("="*70 + "\n")
        
        # Process each group
        processed_count = 0
        created_count = 0
        updated_count = 0
        validation_failures = []
        
        for key, group in grouped_timesheets.items():
            employee_id = group['employee_id']
            pay_period = group['pay_period']
            timesheets = group['timesheets']
            timesheet_ids = [ts.get('id') for ts in timesheets]
            
            print(f"\n{'─'*70}")
            print(f"👤 Employee: {employee_id}")
            print(f"📅 Pay Period: {pay_period['start_date']} to {pay_period['end_date']}")
            print(f"📋 Timesheets: {len(timesheets)}")
            
            try:
                # Validate pay period
                is_valid, error_msg = validate_pay_period(pay_period)
                if not is_valid:
                    print(f"  ❌ ERROR: Invalid pay period - {error_msg}")
                    continue
                
                # Check if payroll record already exists
                existing_payroll = self.find_existing_payroll(employee_id, pay_period)
                
                payroll_id = None
                if existing_payroll:
                    # Update existing payroll
                    result = self.update_payroll_record(existing_payroll, timesheets)
                    payroll_id = existing_payroll.get('id')
                    updated_count += 1
                else:
                    # Create new payroll record
                    result = self.create_payroll_record(employee_id, timesheets, pay_period)
                    payroll_id = result.get('id')
                    created_count += 1
                
                # CRITICAL: Post-upload verification
                if payroll_id:
                    print(f"  🔍 Verifying payroll record {payroll_id}...")
                    is_valid, verify_errors = verify_post_upload(
                        payroll_id,
                        employee_id,
                        pay_period,
                        timesheet_ids
                    )
                    
                    if not is_valid:
                        print(f"  ❌ CRITICAL: Post-upload verification failed for payroll {payroll_id}!")
                        for error in verify_errors:
                            print(f"     {error}")
                        validation_failures.append({
                            'payroll_id': payroll_id,
                            'employee_id': employee_id,
                            'errors': verify_errors
                        })
                        # Don't mark timesheets as processed if verification failed
                        continue
                    else:
                        print(f"  ✅ Post-upload verification passed")
                
                # Only mark timesheets as processed if payroll operation and verification succeeded
                self.mark_timesheets_processed(timesheet_ids)
                processed_count += len(timesheets)
                
            except Exception as e:
                print(f"  ❌ ERROR processing payroll for employee {employee_id}: {e}")
                print(f"  ⚠️  Timesheets NOT marked as processed - will retry on next run")
                # Don't mark timesheets as processed if payroll failed
                continue
        
        print("\n" + "="*70)
        print("✅ PAYROLL PROCESSING COMPLETE")
        print("="*70)
        print(f"📊 Summary:")
        print(f"   • Timesheets Processed: {processed_count}")
        print(f"   • Payroll Records Created: {created_count}")
        print(f"   • Payroll Records Updated: {updated_count}")
        
        if validation_failures:
            print(f"\n⚠️  WARNING: {len(validation_failures)} payroll record(s) failed post-upload verification:")
            for failure in validation_failures:
                print(f"   • Payroll {failure['payroll_id']} (Employee: {failure['employee_id']})")
                for error in failure['errors']:
                    print(f"     - {error}")
            print("\n⚠️  Please review these records manually!")
        
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

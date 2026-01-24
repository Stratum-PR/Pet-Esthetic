"""
Noloco Payroll Processing Script
==================================

Processes approved timesheets and creates/updates payroll records in Noloco.

Key Features:
- Automatic bi-weekly pay period calculation (Monday-Sunday, 14-day periods)
- Handles existing payroll records (updates with new timesheets)
- Reconciles when manager clears approved on a linked timesheet: removes that
  timesheet from payroll and updates hours accordingly
- Validates data integrity before processing
- Preserves employee PIN format (leading zeros)
- Links timesheets to payroll records

Author: Senior Dev Review & Rewrite
Date: 2026-01-23
"""

import requests
import os
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Set
import json
import time
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ============================================================================
# CONFIGURATION
# ============================================================================

API_TOKEN = os.getenv("NOLOCO_API_TOKEN")
PROJECT_ID = os.getenv("NOLOCO_PROJECT_ID")

if not API_TOKEN or not str(API_TOKEN).strip():
    raise Exception("ERROR: NOLOCO_API_TOKEN not set!")
if not PROJECT_ID or not str(PROJECT_ID).strip():
    raise Exception("ERROR: NOLOCO_PROJECT_ID not set!")

API_TOKEN = str(API_TOKEN).strip()
PROJECT_ID = str(PROJECT_ID).strip()
API_URL = f"https://api.portals.noloco.io/data/{PROJECT_ID}"

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
RATE_LIMIT_DELAY = 0.5  # seconds between API calls

# Timezone
PR_TIMEZONE = ZoneInfo('America/Puerto_Rico')

# Pay period reference date (first Monday of the bi-weekly cycle)
REFERENCE_MONDAY = date(2026, 1, 12)  # Jan 12, 2026 was a Monday

# Default payroll values
DEFAULT_PAYMENT_METHOD = 'DIRECT_DEPOSIT'
DEFAULT_PAYROLL_STATUS = 'PENDING'

# ============================================================================
# API CONNECTION
# ============================================================================

def run_graphql_query(query: str, retry_count: int = 0) -> Dict:
    """
    Execute a GraphQL query with retry logic.
    Matches the exact pattern from Noloco_Splash_Page_Timesheet_Updates.py
    
    Args:
        query: GraphQL query string
        retry_count: Current retry attempt
        
    Returns:
        Response data as dictionary
        
    Raises:
        Exception: If query fails after retries
    """
    try:
        # Disable proxy for Noloco API requests
        # Some systems have misconfigured proxy settings that interfere
        proxies = {
            'http': None,
            'https': None
        }
        
        # Debug: Print headers (without full token for security)
        if retry_count == 0:
            print(f"  DEBUG: API URL: {API_URL}")
            print(f"  DEBUG: Token present: {bool(API_TOKEN)}")
            print(f"  DEBUG: Token length: {len(API_TOKEN) if API_TOKEN else 0}")
            print(f"  DEBUG: Headers keys: {list(HEADERS.keys())}")
        
        response = requests.post(
            API_URL,
            headers=HEADERS,
            json={"query": query},
            proxies=proxies,
            timeout=30
        )
        
        # Debug: Print response details
        if retry_count == 0:
            print(f"  DEBUG: Response status: {response.status_code}")
            if response.status_code != 200:
                print(f"  DEBUG: Response text: {response.text[:200]}")
        
        # Handle rate limiting
        if response.status_code == 429:
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_DELAY * (retry_count + 1)
                print(f"  WARNING: Rate limited, waiting {wait_time}s before retry {retry_count + 1}/{MAX_RETRIES}...")
                time.sleep(wait_time)
                return run_graphql_query(query, retry_count + 1)
            else:
                raise Exception(f"Rate limit exceeded after {MAX_RETRIES} retries")
        
        # Handle server errors
        if response.status_code >= 500:
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_DELAY * (retry_count + 1)
                print(f"  WARNING: Server error {response.status_code}, retrying in {wait_time}s...")
                time.sleep(wait_time)
                return run_graphql_query(query, retry_count + 1)
            else:
                raise Exception(f"Server error {response.status_code} after {MAX_RETRIES} retries")
        
        if response.status_code == 401:
            raise Exception("Authentication failed. Check your NOLOCO_API_TOKEN.")
        
        if response.status_code != 200:
            raise Exception(f"API error: {response.status_code} - {response.text}")
        
        result = response.json()
        
        # Debug: Print full response if there are errors
        if "errors" in result:
            if retry_count == 0:
                print(f"  DEBUG: Full response: {json.dumps(result, indent=2)}")
            error_messages = [error.get("message", "Unknown error") for error in result["errors"]]
            raise Exception(f"GraphQL error: {'; '.join(error_messages)}")
        
        return result["data"]
        
    except requests.exceptions.Timeout:
        if retry_count < MAX_RETRIES:
            wait_time = RETRY_DELAY * (retry_count + 1)
            print(f"  WARNING: Request timeout, retrying in {wait_time}s...")
            time.sleep(wait_time)
            return run_graphql_query(query, retry_count + 1)
        else:
            raise Exception(f"Request timeout after {MAX_RETRIES} retries")
    
    except requests.exceptions.ConnectionError:
        if retry_count < MAX_RETRIES:
            wait_time = RETRY_DELAY * (retry_count + 1)
            print(f"  WARNING: Connection error, retrying in {wait_time}s...")
            time.sleep(wait_time)
            return run_graphql_query(query, retry_count + 1)
        else:
            raise Exception(f"Connection error after {MAX_RETRIES} retries")

# ============================================================================
# PAY PERIOD CALCULATION
# ============================================================================

def calculate_biweekly_pay_period(target_date: date) -> Dict[str, str]:
    """
    Calculate the bi-weekly pay period (Monday-Sunday, 14 days) for a given date.
    
    Uses REFERENCE_MONDAY (Jan 12, 2026) as the starting point.
    Pay periods are 14-day cycles starting on Monday.
    
    Args:
        target_date: Date to calculate pay period for
        
    Returns:
        Dictionary with 'start_date' and 'end_date' (YYYY-MM-DD format)
    """
    # Find the Monday of the week containing the target date
    days_since_monday = target_date.weekday()  # 0 = Monday, 6 = Sunday
    monday_of_week = target_date - timedelta(days=days_since_monday)
    
    # Calculate days from reference Monday
    days_from_reference = (monday_of_week - REFERENCE_MONDAY).days
    
    # Find which 14-day period we're in
    period_number = days_from_reference // 14
    period_start = REFERENCE_MONDAY + timedelta(days=period_number * 14)
    period_end = period_start + timedelta(days=13)  # 14 days total (0-13)
    
    return {
        'start_date': period_start.strftime('%Y-%m-%d'),
        'end_date': period_end.strftime('%Y-%m-%d')
    }

def get_current_pay_period() -> Dict[str, str]:
    """Get the current bi-weekly pay period for today."""
    today = date.today()
    return calculate_biweekly_pay_period(today)

def calculate_payment_date(period_end_date: str) -> str:
    """
    Calculate payment date (next Monday after period ends).
    
    Args:
        period_end_date: Period end date (YYYY-MM-DD)
        
    Returns:
        Payment date (YYYY-MM-DD)
    """
    end_date = datetime.strptime(period_end_date, '%Y-%m-%d').date()
    # Find next Monday
    days_until_monday = (7 - end_date.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7  # If it's already Monday, go to next Monday
    payment_date = end_date + timedelta(days=days_until_monday)
    return payment_date.strftime('%Y-%m-%d')

# ============================================================================
# DATA FETCHING
# ============================================================================

def fetch_all_timesheets() -> List[Dict]:
    """
    Fetch all timesheets from Noloco (no filtering in GraphQL).
    
    Returns:
        List of timesheet dictionaries
    """
    all_timesheets = []
    cursor = None
    has_more = True
    
    while has_more:
        if cursor:
            query = f"""
            query {{
                timesheetsCollection(first: 100, after: "{cursor}") {{
                    edges {{
                        node {{
                            id
                            employeePin
                            timesheetDate
                            approved
                            shiftHoursWorked
                            clockDatetime
                            clockOutDatetime
                            payrollRecord {{
                                id
                            }}
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
                            employeePin
                            timesheetDate
                            approved
                            shiftHoursWorked
                            clockDatetime
                            clockOutDatetime
                            payrollRecord {
                                id
                            }
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
            payroll_record = node.get("payrollRecord")
            is_linked = payroll_record is not None and payroll_record.get("id") is not None
            payroll_record_id = payroll_record.get("id") if payroll_record else None
            
            all_timesheets.append({
                "id": node.get("id"),
                "employee_pin": node.get("employeePin"),  # Preserve original format
                "timesheet_date": node.get("timesheetDate"),
                "approved": node.get("approved"),
                "shift_hours_worked": node.get("shiftHoursWorked"),
                "clock_datetime": node.get("clockDatetime"),
                "clock_out_datetime": node.get("clockOutDatetime"),
                "is_linked": is_linked,
                "payroll_record_id": payroll_record_id  # For find_existing_payroll and reconcile
            })
        
        has_more = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")
    
    return all_timesheets

def fetch_all_payroll_records() -> List[Dict]:
    """
    Fetch all payroll records from Noloco.
    
    Returns:
        List of payroll record dictionaries
    """
    all_payroll = []
    cursor = None
    has_more = True
    
    while has_more:
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
                            payRate
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
                            payRate
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
            period_start = node.get("payPeriodStart", "")
            period_end = node.get("payPeriodEnd", "")
            
            # Extract date from ISO datetime string
            period_start_date = period_start.split('T')[0] if period_start else ""
            period_end_date = period_end.split('T')[0] if period_end else ""
            
            all_payroll.append({
                "id": node.get("id"),
                "employee_id": node.get("employeeIdVal"),
                "period_start": period_start_date,
                "period_end": period_end_date,
                "pay_rate": node.get("payRate"),
                "related_timesheet_ids": []  # Will be populated in find_existing_payroll
            })
        
        print(f"  Downloaded page: {len(edges)} records")
        
        has_more = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")
    
    return all_payroll

def fetch_employee_pay_rate(employee_pin: str) -> float:
    """
    Fetch pay rate for an employee.
    
    Args:
        employee_pin: Employee PIN (employeeIdVal)
        
    Returns:
        Pay rate as float, or 0.0 if not found
    """
    all_employees = []
    cursor = None
    has_more = True
    
    while has_more:
        if cursor:
            query = f"""
            query {{
                employeesCollection(first: 100, after: "{cursor}") {{
                    edges {{
                        node {{
                            employeeIdVal
                            payRate
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
                employeesCollection(first: 100) {
                    edges {
                        node {
                            employeeIdVal
                            payRate
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
        collection = data.get("employeesCollection", {})
        edges = collection.get("edges", [])
        page_info = collection.get("pageInfo", {})
        
        for edge in edges:
            all_employees.append(edge.get("node", {}))
        
        has_more = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")
    
    # Find matching employee
    for node in all_employees:
        if str(node.get("employeeIdVal", "")).strip() == str(employee_pin).strip():
            pay_rate = node.get("payRate")
            if pay_rate:
                try:
                    return float(pay_rate)
                except (ValueError, TypeError):
                    pass
    
    return 0.0

# ============================================================================
# DATA PROCESSING
# ============================================================================

def _normalize_id(a) -> str:
    """Compare ids as strings so int/str mismatch doesn't miss matches."""
    if a is None:
        return ""
    return str(a).strip()

def _normalize_period_date(s: str) -> str:
    """Normalize to YYYY-MM-DD for comparison. Handles YYYY-MM-DD and M/D/YYYY."""
    if not s or not isinstance(s, str):
        return s or ""
    s = s.strip().split("T")[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s

def is_approved(ts: Dict) -> bool:
    """
    True only when ts['approved'] is explicitly True or string 'true'/'True'.
    Treats False, None, 'False', '' as not approved.
    """
    v = ts.get("approved")
    if v is True:
        return True
    if isinstance(v, str) and v.strip().lower() == "true":
        return True
    return False

def normalize_employee_pin(employee_pin) -> str:
    """
    Normalize employee PIN to string, preserving original format.
    
    Args:
        employee_pin: Employee PIN (can be string, int, or None)
        
    Returns:
        Normalized employee PIN as string
    """
    if employee_pin is None:
        return None
    return str(employee_pin).strip()

def filter_timesheets_for_period(
    timesheets: List[Dict],
    pay_period: Dict[str, str]
) -> List[Dict]:
    """
    Filter timesheets to only those within the pay period.
    
    Args:
        timesheets: List of all timesheets
        pay_period: Pay period dict with 'start_date' and 'end_date'
        
    Returns:
        Filtered list of timesheets
    """
    period_start = datetime.strptime(pay_period['start_date'], '%Y-%m-%d').date()
    period_end = datetime.strptime(pay_period['end_date'], '%Y-%m-%d').date()
    
    filtered = []
    for ts in timesheets:
        if not is_approved(ts):
            continue
        
        if ts.get('is_linked'):
            continue  # Skip already linked timesheets
        
        ts_date_str = ts.get('timesheet_date', '')
        if not ts_date_str:
            continue
        
        # Extract date from ISO datetime if needed
        ts_date_str = ts_date_str.split('T')[0]
        try:
            ts_date = datetime.strptime(ts_date_str, '%Y-%m-%d').date()
            if period_start <= ts_date <= period_end:
                filtered.append(ts)
        except ValueError:
            continue
    
    return filtered

def group_timesheets_by_employee(
    timesheets: List[Dict],
    pay_period: Dict[str, str]
) -> Dict[str, Dict]:
    """
    Group timesheets by employee PIN.
    
    Args:
        timesheets: List of timesheets
        pay_period: Pay period dict
        
    Returns:
        Dictionary keyed by employee_pin with timesheet lists
    """
    groups = {}
    
    for ts in timesheets:
        employee_pin = normalize_employee_pin(ts.get('employee_pin'))
        if not employee_pin:
            print(f"WARNING: Skipping timesheet {ts.get('id')} - missing employee_pin")
            continue
        
        if employee_pin not in groups:
            groups[employee_pin] = {
                'employee_pin': employee_pin,
                'pay_period': pay_period,
                'timesheets': []
            }
        
        groups[employee_pin]['timesheets'].append(ts)
    
    return groups

def calculate_total_hours(timesheets: List[Dict]) -> float:
    """
    Calculate total hours from timesheets.
    
    Args:
        timesheets: List of timesheet dictionaries
        
    Returns:
        Total hours as float
    """
    total = 0.0
    for ts in timesheets:
        hours = ts.get('shift_hours_worked')
        if hours:
            try:
                total += float(hours)
            except (ValueError, TypeError):
                pass
    return total

# ============================================================================
# VALIDATION
# ============================================================================

def validate_no_duplicate_clock_times(timesheets: List[Dict]) -> Tuple[bool, List[str]]:
    """
    Validate no duplicate clock in/out times within timesheets.
    
    Args:
        timesheets: List of timesheets
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    clock_pairs = {}
    
    for ts in timesheets:
        ts_id = ts.get('id', 'unknown')
        clock_in = ts.get('clock_datetime')
        clock_out = ts.get('clock_out_datetime')
        
        if clock_in and clock_out:
            pair_key = f"{clock_in}|{clock_out}"
            if pair_key in clock_pairs:
                errors.append(
                    f"CRITICAL: Duplicate clock times found! "
                    f"Timesheet {ts_id} has same clock in/out as {clock_pairs[pair_key]}"
                )
            else:
                clock_pairs[pair_key] = ts_id
    
    return len(errors) == 0, errors

def find_existing_payroll(
    employee_pin: str,
    pay_period: Dict[str, str],
    all_payroll_records: List[Dict],
    all_timesheets: List[Dict]
) -> Optional[Dict]:
    """
    Find existing payroll record for employee and pay period.
    Also populates related_timesheet_ids by checking timesheets.
    
    Args:
        employee_pin: Employee PIN
        pay_period: Pay period dict
        all_payroll_records: All payroll records
        all_timesheets: All timesheets to check for relationships
        
    Returns:
        Matching payroll record with related_timesheet_ids populated, or None
    """
    employee_pin = normalize_employee_pin(employee_pin)
    period_start = pay_period['start_date']
    period_end = pay_period['end_date']
    
    for payroll in all_payroll_records:
        payroll_emp_id = normalize_employee_pin(payroll.get('employee_id'))
        if payroll_emp_id == employee_pin:
            if (_normalize_period_date(payroll.get('period_start') or "") == period_start and
                _normalize_period_date(payroll.get('period_end') or "") == period_end):
                payroll_id = payroll.get('id')
                
                # Find timesheets linked to this payroll (compare ids as strings).
                related_ids = []
                pid = _normalize_id(payroll_id)
                for ts in all_timesheets:
                    if _normalize_id(ts.get('payroll_record_id')) == pid:
                        related_ids.append(ts.get('id'))
                
                payroll['related_timesheet_ids'] = related_ids
                return payroll
    
    return None

def compute_correct_timesheet_ids_for_payroll(
    payroll_record: Dict,
    all_timesheets: List[Dict],
    pay_period: Dict[str, str],
    new_timesheet_ids: Optional[List[str]] = None
) -> List[str]:
    """
    Compute the correct set of timesheet IDs for an existing payroll.
    Unlinks non-approved timesheets: we omit them from this set, so when we
    set relatedTimesheetsId to it, they are removed from the payroll relation.
    - Keeps linked timesheets only if still approved and in pay period.
    - Drops (unlinks) linked timesheets whose approved was cleared by manager.
    - Adds any new_timesheet_ids (approved, in-period, not-yet-linked).
    
    Args:
        payroll_record: Existing payroll with related_timesheet_ids
        all_timesheets: All timesheets (for approval and date lookup)
        pay_period: Pay period dict with start_date, end_date
        new_timesheet_ids: IDs of new timesheets to add (from group)
        
    Returns:
        Sorted list of timesheet IDs that should remain linked (for determinism).
    """
    new_timesheet_ids = new_timesheet_ids or []
    ts_by_id = {ts["id"]: ts for ts in all_timesheets if ts.get("id")}
    period_start = datetime.strptime(pay_period["start_date"], "%Y-%m-%d").date()
    period_end = datetime.strptime(pay_period["end_date"], "%Y-%m-%d").date()
    
    related = payroll_record.get("related_timesheet_ids") or []
    kept = []
    for tid in related:
        ts = ts_by_id.get(tid)
        if not ts:
            continue
        # Unlink non-approved: omit so they are removed from relatedTimesheetsId
        if not is_approved(ts):
            continue
        td = (ts.get("timesheet_date") or "").split("T")[0]
        if not td:
            continue
        try:
            d = datetime.strptime(td, "%Y-%m-%d").date()
            if period_start <= d <= period_end:
                kept.append(tid)
        except ValueError:
            continue
    
    # Add new IDs (no dupes), then sort for stable output
    seen = set(kept)
    for tid in new_timesheet_ids:
        if tid and tid not in seen:
            seen.add(tid)
            kept.append(tid)
    return sorted(kept)

# ============================================================================
# PAYROLL OPERATIONS
# ============================================================================

def create_payroll_record(
    employee_pin: str,
    timesheets: List[Dict],
    pay_period: Dict[str, str],
    pay_rate: float
) -> Dict:
    """
    Create a new payroll record.
    
    CRITICAL: Always uses the pay_period parameter (calculated from formula),
    NEVER calculates period from timesheet dates.
    
    Args:
        employee_pin: Employee PIN (must be string with leading zeros preserved)
        timesheets: List of timesheets to link
        pay_period: Pay period dict (MUST be calculated from get_current_pay_period())
        pay_rate: Employee pay rate
        
    Returns:
        Created payroll record dict with 'id'
    """
    employee_pin = normalize_employee_pin(employee_pin)
    if not employee_pin:
        raise Exception("CRITICAL: Cannot create payroll - employee_pin is missing")
    
    # CRITICAL: Validate pay_period is provided and not derived from timesheets
    if not pay_period or 'start_date' not in pay_period or 'end_date' not in pay_period:
        raise Exception("CRITICAL: pay_period must be provided (calculated from formula)")
    
    timesheet_ids = [ts.get('id') for ts in timesheets if ts.get('id')]
    if not timesheet_ids:
        raise Exception("CRITICAL: Cannot create payroll - no valid timesheet IDs")
    
    total_hours = calculate_total_hours(timesheets)
    payment_date = calculate_payment_date(pay_period['end_date'])
    
    # CRITICAL: Use pay_period parameter directly - NEVER calculate from timesheet dates
    # Format dates as ISO datetime strings with timezone
    period_start_dt = datetime.strptime(pay_period['start_date'], '%Y-%m-%d')
    period_start_dt = period_start_dt.replace(hour=0, minute=0, second=0, tzinfo=PR_TIMEZONE)
    
    period_end_dt = datetime.strptime(pay_period['end_date'], '%Y-%m-%d')
    period_end_dt = period_end_dt.replace(hour=0, minute=0, second=0, tzinfo=PR_TIMEZONE)
    
    period_start_iso = period_start_dt.isoformat()
    period_end_iso = period_end_dt.isoformat()
    
    timesheet_ids_str = ', '.join([f'"{tid}"' for tid in timesheet_ids])
    
    mutation = f"""
    mutation {{
        createPayroll(
            employeeIdVal: "{employee_pin}"
            payPeriodStart: "{period_start_iso}"
            payPeriodEnd: "{period_end_iso}"
            payRate: {pay_rate}
            paymentMethod: {DEFAULT_PAYMENT_METHOD}
            status: {DEFAULT_PAYROLL_STATUS}
            relatedTimesheetsId: [{timesheet_ids_str}]
        ) {{
            id
        }}
    }}
    """
    
    result = run_graphql_query(mutation)
    payroll_id = result.get("createPayroll", {}).get("id")
    
    if not payroll_id:
        raise Exception("CRITICAL: Payroll creation failed - no ID returned")
    
    print(f"  Created payroll record {payroll_id} for employee {employee_pin}")
    print(f"    Pay Period: {pay_period['start_date']} to {pay_period['end_date']}")
    print(f"    Payment Date: {payment_date}")
    print(f"    Pay Rate: ${pay_rate:.2f}/hr")
    print(f"    Total Hours: {total_hours:.2f}")
    print(f"    Gross Pay: ${pay_rate * total_hours:.2f}")
    print(f"    Timesheets: {len(timesheets)}")
    
    if RATE_LIMIT_DELAY > 0:
        time.sleep(RATE_LIMIT_DELAY)
    
    return {"id": payroll_id}

def unlink_timesheet_from_payroll(ts_id: str) -> None:
    """
    Clear the timesheet's link to payroll by setting payrollRecordId to null.
    Noloco's updatePayroll(relatedTimesheetsId: [...]) may not always take effect;
    unlinking from the Timesheet side ensures non-approved are removed.
    """
    mutation = f"""
    mutation {{
        updateTimesheets(id: "{ts_id}", payrollRecordId: null) {{
            id
        }}
    }}
    """
    run_graphql_query(mutation)
    if RATE_LIMIT_DELAY > 0:
        time.sleep(RATE_LIMIT_DELAY)

def update_payroll_record(
    payroll_record: Dict,
    correct_timesheet_ids: List[str],
) -> Dict:
    """
    Update existing payroll record with the full correct set of timesheet IDs.
    We (1) unlink removed timesheets from the Timesheet side (payrollRecordId: null)
    and (2) set Payroll's relatedTimesheetsId to the correct list. Noloco's
    relatedTimesheetsId on update may not always take effect; unlinking from the
    Timesheet side ensures non-approved are removed.
    
    Hours and gross (e.g. totalHoursWorked2, grossPay) are Noloco formulas;
    we do not set them—Noloco recalculates from relatedTimesheetsId.
    
    Args:
        payroll_record: Existing payroll record (with related_timesheet_ids)
        correct_timesheet_ids: Full list of timesheet IDs that should be linked
        
    Returns:
        Updated payroll record dict
    """
    payroll_id = payroll_record.get("id")
    existing_set = set(payroll_record.get("related_timesheet_ids", []))
    correct_set = set(correct_timesheet_ids)
    
    if correct_set == existing_set:
        print(f"  Payroll {payroll_id} already up to date, no change")
        return {"id": payroll_id}
    
    removed_ids = existing_set - correct_set
    added = len(correct_set - existing_set)
    
    # 1) Unlink from Timesheet side: set payrollRecordId to null for each removed
    for ts_id in removed_ids:
        if ts_id:
            unlink_timesheet_from_payroll(ts_id)
    
    # 2) Update Payroll's relatedTimesheetsId so it only references correct set
    timesheet_ids_str = ", ".join([f'"{tid}"' for tid in correct_timesheet_ids])
    mutation = f"""
    mutation {{
        updatePayroll(
            id: "{payroll_id}"
            relatedTimesheetsId: [{timesheet_ids_str}]
        ) {{
            id
        }}
    }}
    """
    print(f"  updatePayroll(id={payroll_id}, relatedTimesheetsId=[{len(correct_timesheet_ids)} ids])")
    result = run_graphql_query(mutation)
    updated_id = result.get("updatePayroll", {}).get("id")
    
    if not updated_id:
        raise Exception(f"CRITICAL: Payroll update failed for {payroll_id}")
    
    print(f"  Updated payroll record {payroll_id}")
    if removed_ids:
        print(f"    Unlinked {len(removed_ids)} timesheet(s) from payroll (approved cleared)")
    if added:
        print(f"    Added {added} new timesheet(s)")
    print(f"    Total timesheets: {len(correct_timesheet_ids)}")
    
    if RATE_LIMIT_DELAY > 0:
        time.sleep(RATE_LIMIT_DELAY)
    
    return {"id": updated_id}

# ============================================================================
# MAIN PROCESSING LOGIC
# ============================================================================

def process_payroll():
    """
    Main function to process timesheets and create/update payroll records.
    """
    print("=" * 70)
    print("Pet Esthetic Payroll Processing")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Get current pay period
    current_pay_period = get_current_pay_period()
    print(f"Current Pay Period: {current_pay_period['start_date']} to {current_pay_period['end_date']}\n")
    
    # Fetch all data
    print("Fetching all timesheets...")
    all_timesheets = fetch_all_timesheets()
    print(f"  Total timesheets: {len(all_timesheets)}")
    
    # Filter for current period (approved, not linked, in period)
    period_timesheets = filter_timesheets_for_period(all_timesheets, current_pay_period)
    print(f"  Filtered to {len(period_timesheets)} timesheet(s) within pay period")
    
    if not period_timesheets:
        print("\nNo approved, unprocessed timesheets for current pay period.")
        print("  (Will still reconcile existing payrolls if manager cleared approved on linked timesheets.)")
    
    # Group by employee
    employee_groups = group_timesheets_by_employee(period_timesheets, current_pay_period)
    print(f"\nProcessing {len(employee_groups)} employee(s)\n")
    
    # Fetch all payroll records
    print("Fetching all payroll records...")
    all_payroll_records = fetch_all_payroll_records()
    print(f"  Total payroll records: {len(all_payroll_records)}")
    
    # Populate related_timesheet_ids from timesheets' payroll_record_id.
    # Use _normalize_id so int vs str (e.g. 54 vs "54") still matches.
    for p in all_payroll_records:
        pid = _normalize_id(p.get("id"))
        p["related_timesheet_ids"] = [
            ts["id"] for ts in all_timesheets
            if _normalize_id(ts.get("payroll_record_id")) == pid
        ]
    
    # Process each employee
    created_count = 0
    updated_count = 0
    skipped_count = 0
    
    for employee_pin, group in employee_groups.items():
        timesheets = group['timesheets']
        print(f"\n{'=' * 70}")
        print(f"Employee: {employee_pin}")
        print(f"Pay Period: {current_pay_period['start_date']} to {current_pay_period['end_date']}")
        print(f"Timesheets: {len(timesheets)}")
        print(f"{'=' * 70}")
        
        # Validate no duplicate clock times
        is_valid, errors = validate_no_duplicate_clock_times(timesheets)
        if not is_valid:
            print("  ERROR: Validation failed - duplicate clock times detected")
            for error in errors:
                print(f"    {error}")
            continue
        
        # Check for existing payroll
        existing_payroll = find_existing_payroll(employee_pin, current_pay_period, all_payroll_records, all_timesheets)
        
        if existing_payroll:
            # Compute correct set: still-approved+in-period from linked, plus new from group.
            # Handles manager clearing approved on a linked timesheet (drops it).
            correct = compute_correct_timesheet_ids_for_payroll(
                existing_payroll, all_timesheets, current_pay_period,
                new_timesheet_ids=[ts["id"] for ts in timesheets if ts.get("id")]
            )
            existing_set = set(existing_payroll.get("related_timesheet_ids", []))
            if set(correct) != existing_set:
                try:
                    update_payroll_record(existing_payroll, correct)
                    updated_count += 1
                except Exception as e:
                    print(f"  ERROR: Failed to update payroll: {e}")
            else:
                print(f"  Payroll {existing_payroll.get('id')} up to date")
                skipped_count += 1
            continue
        else:
            # Create new payroll record
            pay_rate = fetch_employee_pay_rate(employee_pin)
            
            if pay_rate == 0.0:
                print(f"  WARNING: Pay rate is 0.0 for employee {employee_pin}")
                print(f"  Continuing anyway - check employee record in Noloco")
            
            try:
                # CRITICAL: Always use current_pay_period (calculated from formula)
                # NEVER use group['pay_period'] as it might have been set incorrectly
                create_payroll_record(employee_pin, timesheets, current_pay_period, pay_rate)
                created_count += 1
            except Exception as e:
                print(f"  ERROR: Failed to create payroll: {e}")
                continue
    
    # Reconcile existing payrolls whose employees have no new timesheets in this run.
    # Handles manager clearing approved on linked timesheets (removes those hours).
    target_start = current_pay_period["start_date"]
    target_end = current_pay_period["end_date"]
    existing_in_period = [
        p for p in all_payroll_records
        if _normalize_period_date(p.get("period_start") or "") == target_start
        and _normalize_period_date(p.get("period_end") or "") == target_end
    ]
    print(f"\nReconcile: {len(existing_in_period)} payroll(s) in current period (excluding those in employee_groups)")
    for p in existing_in_period:
        emp = normalize_employee_pin(p.get("employee_id"))
        if emp in employee_groups:
            continue  # Already handled in loop above
        correct = compute_correct_timesheet_ids_for_payroll(
            p, all_timesheets, current_pay_period, new_timesheet_ids=[]
        )
        if set(correct) != set(p.get("related_timesheet_ids", [])):
            print(f"\n{'=' * 70}")
            print(f"Reconcile (employee {emp}, payroll {p.get('id')}): un-linking non-approved timesheets")
            print(f"{'=' * 70}")
            try:
                update_payroll_record(p, correct)
                updated_count += 1
            except Exception as e:
                print(f"  ERROR: Failed to reconcile payroll: {e}")
        else:
            print(f"  Payroll {p.get('id')} ({emp}): up to date (no non-approved linked to remove)")
    
    # Summary
    print("\n" + "=" * 70)
    print("PAYROLL PROCESSING COMPLETE")
    print("=" * 70)
    print(f"Summary:")
    print(f"  Created: {created_count}")
    print(f"  Updated: {updated_count}")
    print(f"  Skipped: {skipped_count}")
    print("=" * 70)

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        process_payroll()
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user")
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

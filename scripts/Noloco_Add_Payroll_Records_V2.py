"""
Noloco Payroll Processing Script
==================================

Processes approved clock records and creates/updates payroll records in Noloco.

SOURCE TABLE CHANGE (Feb 2026):
  Previously read from: timesheets (timesheetsCollection)
  Now reads from: Splash Page Clocks (testClockingActionCollection)

  Field renames from old source → new source:
    timesheets.clockDatetime     → clocks.clockIn
    timesheets.clockOutDatetime  → clocks.clockOut
    timesheets.shiftHoursWorked  → clocks.shiftHours
    timesheets.timesheetDate     → derived from clocks.clockIn (date portion)
    timesheets.approved          → clocks.approved  (NEW field added to Clocks table)
    timesheets.payrollRecord.id  → clocks.payrollRecord.id  (NEW relationship on Clocks table)

  Fields that stayed the same:
    employeePin, id

Key Features:
- Automatic bi-weekly pay period calculation (Monday-Sunday, 14-day periods)
- Handles existing payroll records (updates with new clock records)
- Reconciles when manager clears approved on a linked clock record
- Validates data integrity before processing
- Preserves employee PIN format (leading zeros)
- Links clock records to payroll records

Author: Stratum PR LLC
Date: 2026-02-17
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
    """Execute a GraphQL query with retry logic."""
    try:
        proxies = {'http': None, 'https': None}

        if retry_count == 0:
            print(f"  DEBUG: API URL: {API_URL}")
            print(f"  DEBUG: Token present: {bool(API_TOKEN)}")
            print(f"  DEBUG: Token length: {len(API_TOKEN) if API_TOKEN else 0}")

        response = requests.post(
            API_URL,
            headers=HEADERS,
            json={"query": query},
            proxies=proxies,
            timeout=30
        )

        if retry_count == 0:
            print(f"  DEBUG: Response status: {response.status_code}")
            if response.status_code != 200:
                print(f"  DEBUG: Response text: {response.text[:200]}")

        if response.status_code == 429:
            if retry_count < MAX_RETRIES:
                wait_time = RETRY_DELAY * (retry_count + 1)
                print(f"  WARNING: Rate limited, waiting {wait_time}s before retry {retry_count + 1}/{MAX_RETRIES}...")
                time.sleep(wait_time)
                return run_graphql_query(query, retry_count + 1)
            else:
                raise Exception(f"Rate limit exceeded after {MAX_RETRIES} retries")

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
# (Unchanged from original)
# ============================================================================

def calculate_biweekly_pay_period(target_date: date) -> Dict[str, str]:
    """Calculate the bi-weekly pay period for a given date."""
    days_since_monday = target_date.weekday()
    monday_of_week = target_date - timedelta(days=days_since_monday)
    days_from_reference = (monday_of_week - REFERENCE_MONDAY).days
    period_number = days_from_reference // 14
    period_start = REFERENCE_MONDAY + timedelta(days=period_number * 14)
    period_end = period_start + timedelta(days=13)
    return {
        'start_date': period_start.strftime('%Y-%m-%d'),
        'end_date': period_end.strftime('%Y-%m-%d')
    }

def get_current_pay_period() -> Dict[str, str]:
    """Get the current bi-weekly pay period. If today is the start of a new
    period, return the previous period so managers can validate before it closes."""
    today = date.today()
    period = calculate_biweekly_pay_period(today)
    period_start = datetime.strptime(period['start_date'], '%Y-%m-%d').date()
    if today == period_start:
        prev_period_start = period_start - timedelta(days=14)
        prev_period_end = prev_period_start + timedelta(days=13)
        return {
            'start_date': prev_period_start.strftime('%Y-%m-%d'),
            'end_date': prev_period_end.strftime('%Y-%m-%d')
        }
    return period

def calculate_payment_date(period_end_date: str) -> str:
    """Calculate payment date (next Monday after period ends)."""
    end_date = datetime.strptime(period_end_date, '%Y-%m-%d').date()
    days_until_monday = (7 - end_date.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    payment_date = end_date + timedelta(days=days_until_monday)
    return payment_date.strftime('%Y-%m-%d')


# ============================================================================
# DATA FETCHING — Updated to read from testClockingAction (Splash Page Clocks)
# ============================================================================

def fetch_all_clock_records() -> List[Dict]:
    """
    Fetch all Splash Page Clock records from Noloco.

    CHANGED FROM ORIGINAL:
      Was: fetch_all_timesheets() → timesheetsCollection
      Now: fetch_all_clock_records() → testClockingActionCollection

    Field mapping from old timesheets query → new clocks query:
      clockDatetime     → clockIn
      clockOutDatetime  → clockOut
      shiftHoursWorked  → shiftHours
      timesheetDate     → derived from clockIn (date only, done in processing)
      approved          → approved  (NEW field on Clocks table)
      payrollRecord.id  → payrollRecord.id  (NEW relationship on Clocks table)
    """
    all_records = []
    cursor = None
    has_more = True

    while has_more:
        if cursor:
            query = f"""
            query {{
                testClockingActionCollection(first: 100, after: "{cursor}") {{
                    edges {{
                        node {{
                            id
                            employeePin
                            clockIn
                            clockOut
                            shiftHours
                            approved
                            payrollRecordId
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
                testClockingActionCollection(first: 100) {
                    edges {
                        node {
                            id
                            employeePin
                            clockIn
                            clockOut
                            shiftHours
                            approved
                            payrollRecordId
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
        collection = data.get("testClockingActionCollection", {})
        edges = collection.get("edges", [])
        page_info = collection.get("pageInfo", {})

        for edge in edges:
            node = edge.get("node", {})
            payroll_record_id = node.get("payrollRecordId")
            is_linked = payroll_record_id is not None

            # Derive timesheetDate equivalent from clockIn date portion
            clock_in_str = node.get("clockIn", "")
            timesheet_date = clock_in_str.split("T")[0] if "T" in clock_in_str else clock_in_str.split(",")[0].strip() if clock_in_str else ""

            all_records.append({
                "id": node.get("id"),
                "employee_pin": node.get("employeePin"),
                # Field renames — internal keys kept the same so rest of script is unchanged
                "timesheet_date": timesheet_date,          # derived from clockIn
                "clock_datetime": node.get("clockIn"),     # was clockDatetime
                "clock_out_datetime": node.get("clockOut"),# was clockOutDatetime
                "shift_hours_worked": node.get("shiftHours"),  # was shiftHoursWorked
                "approved": node.get("approved"),
                "is_linked": is_linked,
                "payroll_record_id": payroll_record_id
            })

        has_more = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")

    return all_records


def fetch_all_payroll_records() -> List[Dict]:
    """Fetch all payroll records. Unchanged from original."""
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
            period_start_date = period_start.split('T')[0] if period_start else ""
            period_end_date = period_end.split('T')[0] if period_end else ""

            all_payroll.append({
                "id": node.get("id"),
                "employee_id": node.get("employeeIdVal"),
                "period_start": period_start_date,
                "period_end": period_end_date,
                "pay_rate": node.get("payRate"),
                "related_timesheet_ids": []
            })

        print(f"  Downloaded page: {len(edges)} records")

        has_more = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")

    return all_payroll


def fetch_employee_pay_rate(employee_pin: str) -> float:
    """Fetch pay rate for an employee. Unchanged from original."""
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
# (All internal logic unchanged — field names were normalized in fetch above)
# ============================================================================

def _normalize_id(a) -> str:
    if a is None:
        return ""
    return str(a).strip()

def _normalize_period_date(s: str) -> str:
    if not s or not isinstance(s, str):
        return s or ""
    s = s.strip().split("T")[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s

def is_approved(record: Dict) -> bool:
    """True only when approved is explicitly True or string 'true'/'True'."""
    v = record.get("approved")
    if v is True:
        return True
    if isinstance(v, str) and v.strip().lower() == "true":
        return True
    return False

def normalize_employee_pin(employee_pin) -> str:
    if employee_pin is None:
        return None
    return str(employee_pin).strip()

def filter_records_for_period(
    records: List[Dict],
    pay_period: Dict[str, str]
) -> List[Dict]:
    """Filter clock records to approved, unlinked records within the pay period."""
    period_start = datetime.strptime(pay_period['start_date'], '%Y-%m-%d').date()
    period_end = datetime.strptime(pay_period['end_date'], '%Y-%m-%d').date()

    filtered = []
    for rec in records:
        if not is_approved(rec):
            continue
        if rec.get('is_linked'):
            continue

        date_str = rec.get('timesheet_date', '')
        if not date_str:
            continue

        date_str = date_str.split('T')[0]
        # Handle M/D/YYYY format from Noloco
        for fmt in ('%Y-%m-%d', '%m/%d/%Y'):
            try:
                rec_date = datetime.strptime(date_str, fmt).date()
                if period_start <= rec_date <= period_end:
                    filtered.append(rec)
                break
            except ValueError:
                continue

    return filtered

def group_records_by_employee(
    records: List[Dict],
    pay_period: Dict[str, str]
) -> Dict[str, Dict]:
    """Group clock records by employee PIN."""
    groups = {}
    for rec in records:
        employee_pin = normalize_employee_pin(rec.get('employee_pin'))
        if not employee_pin:
            print(f"WARNING: Skipping record {rec.get('id')} - missing employee_pin")
            continue

        if employee_pin not in groups:
            groups[employee_pin] = {
                'employee_pin': employee_pin,
                'pay_period': pay_period,
                'timesheets': []
            }
        groups[employee_pin]['timesheets'].append(rec)

    return groups

def calculate_total_hours(records: List[Dict]) -> float:
    """Calculate total hours from clock records."""
    total = 0.0
    for rec in records:
        hours = rec.get('shift_hours_worked')
        if hours:
            try:
                total += float(hours)
            except (ValueError, TypeError):
                pass
    return total

def validate_no_duplicate_clock_times(records: List[Dict]) -> Tuple[bool, List[str]]:
    """Validate no duplicate clock in/out times within a set of records."""
    errors = []
    clock_pairs = {}

    for rec in records:
        rec_id = rec.get('id', 'unknown')
        clock_in = rec.get('clock_datetime')
        clock_out = rec.get('clock_out_datetime')

        if clock_in and clock_out:
            pair_key = f"{clock_in}|{clock_out}"
            if pair_key in clock_pairs:
                errors.append(
                    f"CRITICAL: Duplicate clock times found! "
                    f"Record {rec_id} has same clock in/out as {clock_pairs[pair_key]}"
                )
            else:
                clock_pairs[pair_key] = rec_id

    return len(errors) == 0, errors

def find_existing_payroll(
    employee_pin: str,
    pay_period: Dict[str, str],
    all_payroll_records: List[Dict],
    all_clock_records: List[Dict]
) -> Optional[Dict]:
    """Find existing payroll record for employee and pay period."""
    employee_pin = normalize_employee_pin(employee_pin)
    period_start = pay_period['start_date']
    period_end = pay_period['end_date']

    for payroll in all_payroll_records:
        payroll_emp_id = normalize_employee_pin(payroll.get('employee_id'))
        if payroll_emp_id == employee_pin:
            if (_normalize_period_date(payroll.get('period_start') or "") == period_start and
                    _normalize_period_date(payroll.get('period_end') or "") == period_end):
                payroll_id = payroll.get('id')

                # Find clock records linked to this payroll
                related_ids = []
                pid = _normalize_id(payroll_id)
                for rec in all_clock_records:
                    if _normalize_id(rec.get('payroll_record_id')) == pid:
                        related_ids.append(rec.get('id'))

                payroll['related_timesheet_ids'] = related_ids
                return payroll

    return None

def compute_correct_record_ids_for_payroll(
    payroll_record: Dict,
    all_clock_records: List[Dict],
    pay_period: Dict[str, str],
    new_record_ids: Optional[List[str]] = None
) -> List[str]:
    """
    Compute the correct set of clock record IDs for an existing payroll.
    - Keeps linked records only if still approved and in pay period.
    - Drops records whose approved was cleared by manager.
    - Adds new_record_ids.
    """
    new_record_ids = new_record_ids or []
    rec_by_id = {rec["id"]: rec for rec in all_clock_records if rec.get("id")}
    period_start = datetime.strptime(pay_period["start_date"], "%Y-%m-%d").date()
    period_end = datetime.strptime(pay_period["end_date"], "%Y-%m-%d").date()

    related = payroll_record.get("related_timesheet_ids") or []
    kept = []
    for rid in related:
        rec = rec_by_id.get(rid)
        if not rec:
            continue
        if not is_approved(rec):
            continue
        td = (rec.get("timesheet_date") or "").split("T")[0]
        if not td:
            continue
        for fmt in ('%Y-%m-%d', '%m/%d/%Y'):
            try:
                d = datetime.strptime(td, fmt).date()
                if period_start <= d <= period_end:
                    kept.append(rid)
                break
            except ValueError:
                continue

    seen = set(kept)
    for rid in new_record_ids:
        if rid and rid not in seen:
            seen.add(rid)
            kept.append(rid)
    return sorted(kept)


# ============================================================================
# PAYROLL OPERATIONS
# (Mutation field names unchanged — these write to the Payroll table, not Clocks)
# ============================================================================

def create_payroll_record(
    employee_pin: str,
    records: List[Dict],
    pay_period: Dict[str, str],
    pay_rate: float
) -> Dict:
    """
    Create a new payroll record linking to clock record IDs.

    CHANGED FROM ORIGINAL:
      Was: relatedTimesheetsId → array of timesheet IDs
      Now: relatedTimesheetsId → array of clock record IDs
      (The Payroll table relationship field name stays the same in Noloco —
       it now just points to Clocks records instead of Timesheets records.
       If you renamed it in Noloco, update the mutation field name here.)
    """
    employee_pin = normalize_employee_pin(employee_pin)
    if not employee_pin:
        raise Exception("CRITICAL: Cannot create payroll - employee_pin is missing")

    if not pay_period or 'start_date' not in pay_period or 'end_date' not in pay_period:
        raise Exception("CRITICAL: pay_period must be provided (calculated from formula)")

    record_ids = [rec.get('id') for rec in records if rec.get('id')]
    if not record_ids:
        raise Exception("CRITICAL: Cannot create payroll - no valid record IDs")

    total_hours = calculate_total_hours(records)
    payment_date = calculate_payment_date(pay_period['end_date'])

    period_start_dt = datetime.strptime(pay_period['start_date'], '%Y-%m-%d')
    period_start_dt = period_start_dt.replace(hour=0, minute=0, second=0, tzinfo=PR_TIMEZONE)
    period_end_dt = datetime.strptime(pay_period['end_date'], '%Y-%m-%d')
    period_end_dt = period_end_dt.replace(hour=0, minute=0, second=0, tzinfo=PR_TIMEZONE)

    period_start_iso = period_start_dt.isoformat()
    period_end_iso = period_end_dt.isoformat()

    record_ids_str = ', '.join([f'"{rid}"' for rid in record_ids])

    mutation = f"""
    mutation {{
        createPayroll(
            employeeIdVal: "{employee_pin}"
            payPeriodStart: "{period_start_iso}"
            payPeriodEnd: "{period_end_iso}"
            payRate: {pay_rate}
            paymentMethod: {DEFAULT_PAYMENT_METHOD}
            status: {DEFAULT_PAYROLL_STATUS}
            relatedTimesheetsId: [{record_ids_str}]
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
    print(f"    Clock Records linked: {len(records)}")

    if RATE_LIMIT_DELAY > 0:
        time.sleep(RATE_LIMIT_DELAY)

    return {"id": payroll_id}


def unlink_clock_record_from_payroll(record_id: str) -> None:
    """
    Clear the clock record's link to payroll by setting payrollRecordId to null.

    CHANGED FROM ORIGINAL:
      Was: updateTimesheets(id, payrollRecordId: null)
      Now: updateTestClockingAction(id, payrollRecordId: null)
      (Update the mutation name below to match your Noloco schema's
       update mutation for the Splash Page Clocks table.)
    """
    mutation = f"""
    mutation {{
        updateTestClockingAction(id: "{record_id}", payrollRecordId: null) {{
            id
        }}
    }}
    """
    run_graphql_query(mutation)
    if RATE_LIMIT_DELAY > 0:
        time.sleep(RATE_LIMIT_DELAY)


def update_payroll_record(
    payroll_record: Dict,
    correct_record_ids: List[str],
) -> Dict:
    """
    Update existing payroll record with the full correct set of clock record IDs.
    Unlinks removed records from the Clocks side first, then updates Payroll.
    """
    payroll_id = payroll_record.get("id")
    existing_set = set(payroll_record.get("related_timesheet_ids", []))
    correct_set = set(correct_record_ids)

    if correct_set == existing_set:
        print(f"  Payroll {payroll_id} already up to date, no change")
        return {"id": payroll_id}

    removed_ids = existing_set - correct_set
    added = len(correct_set - existing_set)

    # Unlink from Clocks side first
    for rec_id in removed_ids:
        if rec_id:
            unlink_clock_record_from_payroll(rec_id)

    # Update Payroll's relatedTimesheetsId
    record_ids_str = ", ".join([f'"{rid}"' for rid in correct_record_ids])
    mutation = f"""
    mutation {{
        updatePayroll(
            id: "{payroll_id}"
            relatedTimesheetsId: [{record_ids_str}]
        ) {{
            id
        }}
    }}
    """
    print(f"  updatePayroll(id={payroll_id}, relatedTimesheetsId=[{len(correct_record_ids)} ids])")
    result = run_graphql_query(mutation)
    updated_id = result.get("updatePayroll", {}).get("id")

    if not updated_id:
        raise Exception(f"CRITICAL: Payroll update failed for {payroll_id}")

    print(f"  Updated payroll record {payroll_id}")
    if removed_ids:
        print(f"    Unlinked {len(removed_ids)} clock record(s) from payroll (approved cleared)")
    if added:
        print(f"    Added {added} new clock record(s)")
    print(f"    Total clock records: {len(correct_record_ids)}")

    if RATE_LIMIT_DELAY > 0:
        time.sleep(RATE_LIMIT_DELAY)

    return {"id": updated_id}


# ============================================================================
# MAIN PROCESSING LOGIC
# ============================================================================

def process_payroll():
    """Main function to process clock records and create/update payroll records."""
    print("=" * 70)
    print("Pet Esthetic Payroll Processing")
    print("Source: Splash Page Clocks (testClockingActionCollection)")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    current_pay_period = get_current_pay_period()
    print(f"Current Pay Period: {current_pay_period['start_date']} to {current_pay_period['end_date']}\n")

    # Fetch all data
    print("Fetching all clock records...")
    all_clock_records = fetch_all_clock_records()
    print(f"  Total clock records: {len(all_clock_records)}")

    # Filter for current period (approved, not linked, in period)
    period_records = filter_records_for_period(all_clock_records, current_pay_period)
    print(f"  Filtered to {len(period_records)} clock record(s) within pay period")

    if not period_records:
        print("\nNo approved, unprocessed clock records for current pay period.")
        print("  (Will still reconcile existing payrolls if manager cleared approved.)")

    # Group by employee
    employee_groups = group_records_by_employee(period_records, current_pay_period)
    print(f"\nProcessing {len(employee_groups)} employee(s)\n")

    # Fetch all payroll records
    print("Fetching all payroll records...")
    all_payroll_records = fetch_all_payroll_records()
    print(f"  Total payroll records: {len(all_payroll_records)}")

    # Populate related_timesheet_ids from clock records' payroll_record_id
    for p in all_payroll_records:
        pid = _normalize_id(p.get("id"))
        p["related_timesheet_ids"] = [
            rec["id"] for rec in all_clock_records
            if _normalize_id(rec.get("payroll_record_id")) == pid
        ]

    # Process each employee
    created_count = 0
    updated_count = 0
    skipped_count = 0

    for employee_pin, group in employee_groups.items():
        records = group['timesheets']
        print(f"\n{'=' * 70}")
        print(f"Employee: {employee_pin}")
        print(f"Pay Period: {current_pay_period['start_date']} to {current_pay_period['end_date']}")
        print(f"Clock Records: {len(records)}")
        print(f"{'=' * 70}")

        # Validate no duplicate clock times
        is_valid, errors = validate_no_duplicate_clock_times(records)
        if not is_valid:
            print("  ERROR: Validation failed - duplicate clock times detected")
            for error in errors:
                print(f"    {error}")
            continue

        # Check for existing payroll
        existing_payroll = find_existing_payroll(
            employee_pin, current_pay_period, all_payroll_records, all_clock_records
        )

        if existing_payroll:
            correct = compute_correct_record_ids_for_payroll(
                existing_payroll, all_clock_records, current_pay_period,
                new_record_ids=[rec["id"] for rec in records if rec.get("id")]
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
            pay_rate = fetch_employee_pay_rate(employee_pin)
            if pay_rate == 0.0:
                print(f"  WARNING: Pay rate is 0.0 for employee {employee_pin}")
                print(f"  Continuing anyway - check employee record in Noloco")

            try:
                create_payroll_record(employee_pin, records, current_pay_period, pay_rate)
                created_count += 1
            except Exception as e:
                print(f"  ERROR: Failed to create payroll: {e}")
                continue

    # Reconcile existing payrolls for employees with no new records this run
    target_start = current_pay_period["start_date"]
    target_end = current_pay_period["end_date"]
    existing_in_period = [
        p for p in all_payroll_records
        if _normalize_period_date(p.get("period_start") or "") == target_start
        and _normalize_period_date(p.get("period_end") or "") == target_end
    ]
    print(f"\nReconcile: {len(existing_in_period)} payroll(s) in current period")
    for p in existing_in_period:
        emp = normalize_employee_pin(p.get("employee_id"))
        if emp in employee_groups:
            continue
        correct = compute_correct_record_ids_for_payroll(
            p, all_clock_records, current_pay_period, new_record_ids=[]
        )
        if set(correct) != set(p.get("related_timesheet_ids", [])):
            print(f"\n{'=' * 70}")
            print(f"Reconcile (employee {emp}, payroll {p.get('id')}): unlinking non-approved records")
            print(f"{'=' * 70}")
            try:
                update_payroll_record(p, correct)
                updated_count += 1
            except Exception as e:
                print(f"  ERROR: Failed to reconcile payroll: {e}")
        else:
            print(f"  Payroll {p.get('id')} ({emp}): up to date")

    # Summary
    print("\n" + "=" * 70)
    print("PAYROLL PROCESSING COMPLETE")
    print("=" * 70)
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

"""
Noloco Splash Page Timesheet Updates
Syncs records from Test Clocking Action table to Timesheets table
Generates email reports for any issues found
"""
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import time
import os
from jinja2 import Environment, FileSystemLoader, select_autoescape

# Import local modules
from tools import send_gmail
from config import Config

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def normalize_datetime_for_comparison(dt_string):
    """
    Normalize any datetime string to UTC for comparison purposes.
    Removes timezone offsets, milliseconds, and converts everything to UTC time.
    """
    if not dt_string or pd.isna(dt_string):
        return None
    
    try:
        dt_string = str(dt_string).strip()
        
        if dt_string.endswith('Z'):
            clean_string = dt_string.replace('Z', '').split('.')[0]
            dt = datetime.fromisoformat(clean_string)
            dt = dt.replace(tzinfo=ZoneInfo('UTC'))
        elif '+' in dt_string or dt_string.count('-') > 2:
            dt = datetime.fromisoformat(dt_string)
            dt = dt.astimezone(ZoneInfo('UTC'))
        else:
            clean_string = dt_string.split('.')[0]
            dt = datetime.fromisoformat(clean_string)
            dt = dt.replace(tzinfo=ZoneInfo('UTC'))
        
        return dt.strftime('%Y-%m-%d %H:%M:%S')
        
    except Exception as e:
        print(f"  ⚠️  Warning: Could not normalize datetime '{dt_string}': {str(e)}")
        return None


def format_datetime_for_email(dt_string):
    """
    Format datetime string for email display as: Jan-14, 2026 9:03 AM
    Converts to Puerto Rico timezone first.
    """
    if not dt_string or pd.isna(dt_string):
        return 'N/A'
    
    try:
        dt_string = str(dt_string).strip()
        
        # Parse datetime
        if dt_string.endswith('Z'):
            clean_string = dt_string.replace('Z', '').split('.')[0]
            dt = datetime.fromisoformat(clean_string)
            dt = dt.replace(tzinfo=ZoneInfo('UTC'))
        elif '+' in dt_string or dt_string.count('-') > 2:
            dt = datetime.fromisoformat(dt_string)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo('UTC'))
        else:
            clean_string = dt_string.split('.')[0]
            dt = datetime.fromisoformat(clean_string)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo('UTC'))
        
        # Convert to Puerto Rico timezone
        pr_dt = dt.astimezone(ZoneInfo('America/Puerto_Rico'))
        
        # Format as: Jan-14, 2026 9:03 AM
        return pr_dt.strftime('%b-%d, %Y %I:%M %p')
        
    except Exception as e:
        print(f"  ⚠️  Warning: Could not format datetime '{dt_string}': {str(e)}")
        return str(dt_string)


def convert_utc_to_pr(utc_datetime_string):
    """Convert UTC datetime string to Puerto Rico timezone (UTC-4)"""
    try:
        clean_string = utc_datetime_string.replace('Z', '').split('.')[0]
        utc_dt = datetime.fromisoformat(clean_string)
        utc_dt = utc_dt.replace(tzinfo=ZoneInfo('UTC'))
        pr_dt = utc_dt.astimezone(ZoneInfo('America/Puerto_Rico'))
        return pr_dt.isoformat()
    except Exception as e:
        raise Exception(f"Failed to convert datetime '{utc_datetime_string}': {str(e)}")


def run_graphql_query(config, query, retry_count=0):
    """Send a GraphQL query to Noloco API with retry logic"""
    try:
        # Disable proxy for Noloco API requests
        # Some systems have misconfigured proxy settings that interfere
        proxies = {
            'http': None,
            'https': None
        }
        
        response = requests.post(
            config.api_url,
            headers=config.headers,
            json={"query": query},
            proxies=proxies,
            timeout=config.request_timeout
        )
        
        # Handle rate limiting
        if response.status_code == 429:
            if retry_count < config.max_retries:
                wait_time = config.retry_delay * (retry_count + 1)
                print(f"  ⚠️  Rate limited, waiting {wait_time}s before retry {retry_count + 1}/{config.max_retries}...")
                time.sleep(wait_time)
                return run_graphql_query(config, query, retry_count + 1)
            else:
                raise Exception(f"Rate limit exceeded after {config.max_retries} retries")
        
        # Handle server errors
        if response.status_code >= 500:
            if retry_count < config.max_retries:
                wait_time = config.retry_delay * (retry_count + 1)
                print(f"  ⚠️  Server error {response.status_code}, retrying in {wait_time}s...")
                time.sleep(wait_time)
                return run_graphql_query(config, query, retry_count + 1)
            else:
                raise Exception(f"Server error {response.status_code} after {config.max_retries} retries")
        
        if response.status_code == 401:
            raise Exception("Authentication failed. Check your NOLOCO_API_TOKEN.")
        
        if response.status_code != 200:
            raise Exception(f"API error: {response.status_code} - {response.text}")
        
        result = response.json()
        
        if "errors" in result:
            error_messages = [error.get("message", "Unknown error") for error in result["errors"]]
            raise Exception(f"GraphQL error: {'; '.join(error_messages)}")
        
        return result["data"]
        
    except requests.exceptions.Timeout:
        if retry_count < config.max_retries:
            wait_time = config.retry_delay * (retry_count + 1)
            print(f"  ⚠️  Request timeout, retrying in {wait_time}s...")
            time.sleep(wait_time)
            return run_graphql_query(config, query, retry_count + 1)
        else:
            raise Exception(f"Request timeout after {config.max_retries} retries")
    
    except requests.exceptions.ConnectionError:
        if retry_count < config.max_retries:
            wait_time = config.retry_delay * (retry_count + 1)
            print(f"  ⚠️  Connection error, retrying in {wait_time}s...")
            time.sleep(wait_time)
            return run_graphql_query(config, query, retry_count + 1)
        else:
            raise Exception(f"Connection error after {config.max_retries} retries")


def fetch_all_records(config, collection_name, fields):
    """Generic function to fetch all records from any Noloco collection"""
    all_records = []
    has_more_pages = True
    cursor = None
    page_number = 1
    
    # Build field selection for GraphQL
    field_selection = "\n".join([f"                                {field}" for field in fields])
    
    while has_more_pages:
        if cursor:
            query = f"""
            query {{
                {collection_name}Collection(first: 100, after: "{cursor}") {{
                    edges {{
                        node {{
{field_selection}
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
                {collection_name}Collection(first: 100) {{
                    edges {{
                        node {{
{field_selection}
                        }}
                    }}
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                }}
            }}
            """
        
        data = run_graphql_query(config, query)
        collection = data.get(f"{collection_name}Collection", {})
        edges = collection.get("edges", [])
        page_info = collection.get("pageInfo", {})
        
        for edge in edges:
            all_records.append(edge.get("node", {}))
        
        print(f"  Downloaded page {page_number}: {len(edges)} records")
        
        has_more_pages = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")
        page_number += 1
    
    return pd.DataFrame(all_records)


# ============================================================================
# MAIN SYNC FUNCTIONS
# ============================================================================

def download_test_clocking_actions(config):
    """Download and filter Splash Page Clocks records"""
    print("=" * 80)
    print("STEP 1: Downloading Splash Page Clocks (testClockingAction) records...")
    print("=" * 80)
    
    df = fetch_all_records(
        config,
        "testClockingAction",
        ["id", "employeeIdVal", "employeePin", "employeeFullName", "clockIn", "clockOut"]
    )
    
    if len(df) == 0:
        print("  ⚠️  Warning: No records found in Splash Page Clocks table")
        return df
    
    print(f"\n  Total records downloaded: {len(df)}")
    
    # Filter for complete records
    print("\n" + "=" * 80)
    print("STEP 2: Filtering records - keeping only complete records...")
    print("=" * 80)
    
    initial_count = len(df)
    missing_pin = df["employeePin"].isna() | (df["employeePin"] == "")
    missing_clock_in = df["clockIn"].isna() | (df["clockIn"] == "")
    missing_clock_out = df["clockOut"].isna() | (df["clockOut"] == "")
    
    print(f"  Records missing Employee Pin: {missing_pin.sum()}")
    print(f"  Records missing Clock In: {missing_clock_in.sum()}")
    print(f"  Records missing Clock Out: {missing_clock_out.sum()}")
    
    df = df[~(missing_pin | missing_clock_in | missing_clock_out)].copy()
    
    print(f"\n  ✓ Filtered out {initial_count - len(df)} incomplete records")
    print(f"  ✓ Valid records remaining: {len(df)}")
    
    if len(df) == 0:
        return df
    
    # Normalize datetime fields
    print("\n  Normalizing datetime fields for comparison...")
    df["clock_in_normalized"] = df["clockIn"].apply(normalize_datetime_for_comparison)
    df["clock_out_normalized"] = df["clockOut"].apply(normalize_datetime_for_comparison)
    
    normalization_failed = df["clock_in_normalized"].isna() | df["clock_out_normalized"].isna()
    if normalization_failed.any():
        print(f"  ⚠️  Warning: {normalization_failed.sum()} records failed datetime normalization")
        df = df[~normalization_failed].copy()
        print(f"  ✓ Valid records after normalization: {len(df)}")
    
    return df


def check_missing_clock_out(config):
    """Check for records missing clock out with >8 hours since clock in"""
    print("\n" + "=" * 80)
    print("CHECKING FOR MISSING CLOCK OUTS (>8 hours)")
    print("=" * 80)
    
    df = fetch_all_records(
        config,
        "testClockingAction",
        ["id", "employeeIdVal", "employeePin", "employeeFullName", "clockIn", "clockOut"]
    )
    
    if len(df) == 0:
        print("  No records to check")
        return pd.DataFrame()
    
    # Filter for records missing clock out
    missing_clock_out = df["clockOut"].isna() | (df["clockOut"] == "")
    records_without_clock_out = df[missing_clock_out].copy()
    
    if len(records_without_clock_out) == 0:
        print("  ✓ No records missing clock out")
        return pd.DataFrame()
    
    print(f"  Found {len(records_without_clock_out)} records missing clock out")
    
    # Get current time in Puerto Rico timezone
    now_pr = datetime.now(ZoneInfo('America/Puerto_Rico'))
    
    # Check how long ago they clocked in
    problematic_records = []
    
    for idx, row in records_without_clock_out.iterrows():
        try:
            clock_in_str = row['clockIn']
            if not clock_in_str or pd.isna(clock_in_str):
                continue
            
            # Convert to PR timezone
            if clock_in_str.endswith('Z'):
                clock_in_clean = clock_in_str.replace('Z', '').split('.')[0]
                clock_in_utc = datetime.fromisoformat(clock_in_clean).replace(tzinfo=ZoneInfo('UTC'))
            else:
                clock_in_utc = datetime.fromisoformat(clock_in_str)
            
            clock_in_pr = clock_in_utc.astimezone(ZoneInfo('America/Puerto_Rico'))
            
            # Calculate hours since clock in
            time_diff = now_pr - clock_in_pr
            hours_since = time_diff.total_seconds() / 3600
            
            # Flag if more than 8 hours
            if hours_since > 8:
                problematic_records.append({
                    'id': row['id'],
                    'employee_id': row['employeeIdVal'],
                    'employee_pin': row['employeePin'],
                    'employeeFullName': row.get('employeeFullName'),  # Keep original field name from testClockingAction
                    'employee_full_name': row.get('employeeFullName'),  # Also keep for backward compatibility
                    'clock_in': clock_in_pr.strftime('%Y-%m-%d %H:%M:%S'),
                    'clock_in_original': row['clockIn'],
                    'hours_since_clock_in': round(hours_since, 1)
                })
        
        except Exception as e:
            print(f"  ⚠️  Error processing record {row.get('id')}: {str(e)}")
            continue
    
    if len(problematic_records) > 0:
        print(f"\n  ⚠️  ALERT: {len(problematic_records)} records missing clock out for >8 hours!")
        print(f"\n  {'Employee PIN':<15} {'Clock In (PR)':<25} {'Hours Since':<15}")
        print(f"  {'-'*55}")
        
        for record in problematic_records[:10]:
            print(f"  {record['employee_pin']:<15} {record['clock_in']:<25} {record['hours_since_clock_in']:<15.1f}")
        
        if len(problematic_records) > 10:
            print(f"  ... and {len(problematic_records) - 10} more")
    else:
        print("  ✓ No records with missing clock out >8 hours")
    
    # Create DataFrame and ensure employeeFullName column is preserved
    df_result = pd.DataFrame(problematic_records)
    if len(df_result) > 0 and 'employeeFullName' not in df_result.columns:
        # If employeeFullName column is missing, try to add it from employee_full_name
        if 'employee_full_name' in df_result.columns:
            df_result['employeeFullName'] = df_result['employee_full_name']
    
    return df_result


def download_timesheets(config):
    """Download Timesheets records"""
    print("\n" + "=" * 80)
    print("STEP 3: Downloading Timesheets records...")
    print("=" * 80)
    
    df = fetch_all_records(
        config,
        "timesheets",
        ["id", "employeePin", "clockDatetime", "clockOutDatetime"]
    )
    
    print(f"\n  ✓ Total records downloaded: {len(df)}")
    
    if len(df) == 0:
        print("  ⚠️  Note: Timesheets table is empty")
        return df
    
    # Normalize datetime fields
    print("\n  Normalizing datetime fields for comparison...")
    df["clock_in_normalized"] = df["clockDatetime"].apply(normalize_datetime_for_comparison)
    df["clock_out_normalized"] = df["clockOutDatetime"].apply(normalize_datetime_for_comparison)
    
    return df


def get_employee_pin_mapping(config):
    """Fetch employee PIN to record ID mapping and names"""
    print("\n" + "=" * 80)
    print("Fetching Employee records to map employee PINs...")
    print("=" * 80)
    
    # Try to fetch with different possible field names
    try:
        df = fetch_all_records(config, "employees", ["id", "employeePin", "name"])
        name_field = "name"
    except:
        try:
            df = fetch_all_records(config, "employees", ["id", "employeePin", "employeeName"])
            name_field = "employeeName"
        except:
            try:
                df = fetch_all_records(config, "employees", ["id", "employeePin", "fullName"])
                name_field = "fullName"
            except:
                print("  ⚠️  Could not fetch employee names - will use PINs only")
                df = fetch_all_records(config, "employees", ["id", "employeePin"])
                name_field = None
    
    # Filter for employees with PINs
    df = df[df["employeePin"].notna()].copy()
    
    # Create mapping dictionaries
    id_mapping = dict(zip(df["employeePin"], df["id"]))
    
    if name_field and name_field in df.columns:
        name_mapping = dict(zip(df["employeePin"], df[name_field]))
        print(f"  ✓ Found {len(id_mapping)} employees with PINs and names")
    else:
        # Fallback: use PIN as name
        name_mapping = dict(zip(df["employeePin"], df["employeePin"]))
        print(f"  ✓ Found {len(id_mapping)} employees with PINs (names not available)")
    
    if len(id_mapping) == 0:
        print("  ⚠️  WARNING: No employees found with PINs!")
    
    return id_mapping, name_mapping


def get_employee_fullname_mapping_from_clocking(clocking_df):
    """Create employeeFullName mapping from clocking records (preferred over employee table)"""
    if len(clocking_df) == 0 or 'employeeFullName' not in clocking_df.columns:
        return {}
    
    # Create mapping from PIN to employeeFullName, keeping the most recent non-null value
    mapping = {}
    for idx, row in clocking_df.iterrows():
        pin = row.get('employeePin')
        full_name = row.get('employeeFullName')
        
        if pin and pd.notna(pin) and full_name and pd.notna(full_name) and str(full_name).strip():
            # Only update if we don't have a value yet, or if this is a newer record
            if pin not in mapping or not mapping[pin]:
                mapping[pin] = str(full_name).strip()
    
    return mapping


def resolve_employee_name(pin, employee_fullname_mapping, employee_name_mapping, record=None):
    """Helper function to resolve employee name from multiple sources"""
    employee_name = None
    
    # Prefer employeeFullName from clocking records, fallback to employee table mapping
    if pin in employee_fullname_mapping:
        employee_name = employee_fullname_mapping[pin]
    elif pin in employee_name_mapping:
        employee_name = employee_name_mapping[pin]
    elif record and 'employee_full_name' in record and record['employee_full_name']:
        employee_name = record['employee_full_name']
    
    # Only return if we have a valid name (not None, not empty, not 'Unknown')
    if employee_name and str(employee_name).strip() and str(employee_name) != 'Unknown':
        return str(employee_name).strip()
    else:
        return None


def format_normalized_datetime(normalized_dt_string):
    """Format a normalized datetime string (YYYY-MM-DD HH:MM:SS) to email format"""
    if not normalized_dt_string:
        return 'N/A'
    
    try:
        dt = datetime.strptime(str(normalized_dt_string), '%Y-%m-%d %H:%M:%S')
        dt = dt.replace(tzinfo=ZoneInfo('UTC'))
        pr_dt = dt.astimezone(ZoneInfo('America/Puerto_Rico'))
        return pr_dt.strftime('%b-%d, %Y %I:%M %p')
    except:
        return 'N/A'


def find_missing_records(clocking_df, timesheets_df):
    """Find records in Splash Page Clocks that don't exist in Timesheets"""
    print("\n" + "=" * 80)
    print("STEP 5: Comparing tables to find missing records...")
    print("=" * 80)
    
    if len(clocking_df) == 0:
        print("  No records to check from Splash Page Clocks")
        return pd.DataFrame()
    
    # Create match keys
    clocking_df["match_key"] = (
        clocking_df["employeePin"].astype(str) + "_" +
        clocking_df["clock_in_normalized"].astype(str) + "_" +
        clocking_df["clock_out_normalized"].astype(str)
    )
    
    print(f"  Splash Page Clocks records to check: {len(clocking_df)}")
    
    if len(timesheets_df) > 0:
        timesheets_df["match_key"] = (
            timesheets_df["employeePin"].astype(str) + "_" +
            timesheets_df["clock_in_normalized"].astype(str) + "_" +
            timesheets_df["clock_out_normalized"].astype(str)
        )
        
        print(f"  Existing Timesheets records: {len(timesheets_df)}")
        
        if len(timesheets_df) > len(clocking_df):
            print(f"\n  ⚠️  WARNING: Timesheets has MORE records than Splash Page Clocks!")
            print(f"              This indicates potential data integrity issues.")
        
        missing_records = clocking_df[~clocking_df["match_key"].isin(timesheets_df["match_key"])].copy()
    else:
        print("  Existing Timesheets records: 0 (table is empty)")
        missing_records = clocking_df.copy()
    
    missing_records = missing_records.drop(columns=["match_key"])
    
    print(f"\n  ✓ Missing records found: {len(missing_records)}")
    
    if len(missing_records) > 0:
        print("\n  Preview of missing records:")
        preview = missing_records[["employeePin", "clockIn", "clockOut"]].head(3)
        print(preview.to_string(index=False))
    
    return missing_records


def validate_comparison(clocking_df, timesheets_df, missing_df):
    """Validate the comparison logic and find orphaned records"""
    print("\n" + "=" * 80)
    print("STEP 6: POST-COMPARISON VALIDATION")
    print("=" * 80)
    
    validation_passed = True
    orphaned_records_df = pd.DataFrame()
    
    # Check if Timesheets has more records than source
    if len(timesheets_df) > len(clocking_df):
        print(f"  ✗ FAIL: Timesheets has MORE records ({len(timesheets_df)}) than Splash Page Clocks ({len(clocking_df)})!")
        validation_passed = False
        
        # Find orphaned records
        if len(clocking_df) > 0:
            clocking_keys = set(
                clocking_df["employeePin"].astype(str) + "_" +
                clocking_df["clock_in_normalized"].astype(str) + "_" +
                clocking_df["clock_out_normalized"].astype(str)
            )
            
            timesheets_df["match_key_temp"] = (
                timesheets_df["employeePin"].astype(str) + "_" +
                timesheets_df["clock_in_normalized"].astype(str) + "_" +
                timesheets_df["clock_out_normalized"].astype(str)
            )
            
            orphaned_records_df = timesheets_df[~timesheets_df["match_key_temp"].isin(clocking_keys)].copy()
            orphaned_records_df = orphaned_records_df.drop(columns=["match_key_temp"])
            
            if len(orphaned_records_df) > 0:
                print(f"         Found {len(orphaned_records_df)} orphaned records")
    else:
        print(f"  ✓ PASS: Timesheets records ({len(timesheets_df)}) ≤ Source records ({len(clocking_df)})")
    
    if len(missing_df) > len(clocking_df):
        print("  ✗ FAIL: Missing records count exceeds source records count!")
        validation_passed = False
    else:
        print(f"  ✓ PASS: Missing records ({len(missing_df)}) ≤ Source records ({len(clocking_df)})")
    
    print("\n" + "=" * 80)
    if validation_passed:
        print("  ✓✓✓ VALIDATION PASSED")
    else:
        print("  ✗✗✗ VALIDATION FAILED - Review errors above!")
    print("=" * 80)
    
    return validation_passed, orphaned_records_df


def validate_work_hours(records_df):
    """Validate work shifts are not longer than 8 hours"""
    if len(records_df) == 0:
        return records_df, pd.DataFrame()
    
    print("\n" + "=" * 80)
    print("WORK HOURS VALIDATION")
    print("=" * 80)
    
    flagged_records = []
    
    for idx, row in records_df.iterrows():
        try:
            clock_in_str = row['clockIn']
            clock_out_str = row['clockOut']
            
            # Parse datetimes
            if clock_in_str.endswith('Z'):
                clock_in_dt = datetime.fromisoformat(clock_in_str.replace('Z', '').split('.')[0]).replace(tzinfo=ZoneInfo('UTC'))
            else:
                clock_in_dt = datetime.fromisoformat(clock_in_str)
            
            if clock_out_str.endswith('Z'):
                clock_out_dt = datetime.fromisoformat(clock_out_str.replace('Z', '').split('.')[0]).replace(tzinfo=ZoneInfo('UTC'))
            else:
                clock_out_dt = datetime.fromisoformat(clock_out_str)
            
            # Calculate hours
            hours_worked = (clock_out_dt - clock_in_dt).total_seconds() / 3600
            
            if hours_worked > 8:
                flagged_records.append({
                    'employee_pin': row['employeePin'],
                    'clock_in': row['clock_in_normalized'],
                    'clock_out': row['clock_out_normalized'],
                    'hours_worked': round(hours_worked, 2)
                })
        except Exception as e:
            print(f"  ⚠️  Warning: Could not calculate hours for record {idx}: {str(e)}")
    
    if len(flagged_records) > 0:
        print(f"\n  ⚠️  WARNING: Found {len(flagged_records)} records with shifts LONGER than 8 hours!")
        print(f"\n  {'Employee PIN':<15} {'Clock In':<25} {'Clock Out':<25} {'Hours':<10}")
        print(f"  {'-'*75}")
        
        for record in flagged_records[:20]:
            print(f"  {record['employee_pin']:<15} {record['clock_in']:<25} {record['clock_out']:<25} {record['hours_worked']:<10.2f}")
        
        if len(flagged_records) > 20:
            print(f"  ... and {len(flagged_records) - 20} more flagged records")
    else:
        print(f"  ✓ PASS: All records have work shifts ≤ 8 hours")
    
    flagged_df = pd.DataFrame(flagged_records) if flagged_records else pd.DataFrame()
    return records_df, flagged_df


def upload_to_timesheets(config, records_df, employee_pin_mapping):
    """Upload new records to Timesheets table"""
    if len(records_df) == 0:
        print("\n" + "=" * 80)
        print("STEP 7: No records to upload")
        print("=" * 80)
        return 0, {}
    
    print("\n" + "=" * 80)
    print(f"STEP 7: Uploading {len(records_df)} records to Timesheets...")
    print("=" * 80)
    
    created_count = 0
    failed_reasons = {}
    
    for index, row in records_df.iterrows():
        try:
            # Convert to PR timezone
            clock_in_pr = convert_utc_to_pr(row['clockIn'])
            clock_out_pr = convert_utc_to_pr(row['clockOut'])
            
            date_only = clock_in_pr.split('T')[0]
            timesheet_date = f"{date_only}T00:00:00-04:00"
            
            employee_record_id = employee_pin_mapping.get(row['employeePin'])
            
            if not employee_record_id:
                reason = f"No employee found for PIN {row['employeePin']}"
                print(f"  ⚠️  Skipping record {index + 1}: {reason}")
                failed_reasons[reason] = failed_reasons.get(reason, 0) + 1
                continue
            
            create_mutation = f"""
            mutation {{
                createTimesheets(
                    employeeIdVal: "{row['employeeIdVal']}",
                    employeePin: "{row['employeePin']}",
                    clockDatetime: "{clock_in_pr}",
                    clockOutDatetime: "{clock_out_pr}",
                    timesheetDate: "{timesheet_date}",
                    relatedEmployeeId: "{employee_record_id}"
                ) {{
                    id
                }}
            }}
            """
            
            result = run_graphql_query(config, create_mutation)
            created_count += 1
            print(f"  ✓ Created record {created_count}/{len(records_df)}: PIN {row['employeePin']}")
            
            if config.rate_limit_delay > 0 and created_count < len(records_df):
                time.sleep(config.rate_limit_delay)
                
        except Exception as e:
            error_msg = str(e)
            if "Schema error" in error_msg:
                reason = "Schema error"
            elif "Duplicate record" in error_msg:
                reason = "Duplicate record"
            else:
                reason = "API error"
            
            print(f"  ✗ Failed record {index + 1}: {error_msg}")
            failed_reasons[reason] = failed_reasons.get(reason, 0) + 1
    
    print("\n  Upload Summary:")
    print(f"  ✓ Successfully created: {created_count}")
    if failed_reasons:
        print(f"  ✗ Failed: {sum(failed_reasons.values())}")
    
    return created_count, failed_reasons


def create_issues_excel(missing_clock_out_df, orphaned_records_df, flagged_hours_df, failed_reasons):
    """Create Excel file with multiple sheets for each issue type"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'/tmp/timesheet_issues_{timestamp}.xlsx'
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        if len(missing_clock_out_df) > 0:
            missing_clock_out_df.to_excel(writer, sheet_name='Missing Clock Out', index=False)
        
        if len(orphaned_records_df) > 0:
            orphaned_records_df.to_excel(writer, sheet_name='Orphaned Records', index=False)
        
        if len(flagged_hours_df) > 0:
            flagged_hours_df.to_excel(writer, sheet_name='Long Shifts', index=False)
        
        if len(failed_reasons) > 0:
            failed_df = pd.DataFrame([
                {'Failure Reason': reason, 'Count': count}
                for reason, count in failed_reasons.items()
            ])
            failed_df.to_excel(writer, sheet_name='Failed Uploads', index=False)
    
    print(f"  ✓ Excel file created: {filename}")
    return filename


def generate_email_report(clocking_df, timesheets_df, missing_df, created_count,
                         orphaned_records_df, flagged_hours_df, failed_reasons,
                         validation_passed, missing_clock_out_df, employee_name_mapping):
    """Generate HTML email report using Jinja2 template"""
    
    # Get employeeFullName mapping from clocking records (preferred)
    employee_fullname_mapping = get_employee_fullname_mapping_from_clocking(clocking_df)
    
    # Also get employeeFullName from missing_clock_out_df (they come from splash page clocks - testClockingAction)
    if len(missing_clock_out_df) > 0:
        for idx, row in missing_clock_out_df.iterrows():
            pin = row.get('employee_pin')
            # Try both field names (employeeFullName from testClockingAction, employee_full_name as fallback)
            full_name = None
            if 'employeeFullName' in row and pd.notna(row.get('employeeFullName')):
                full_name = row.get('employeeFullName')
            elif 'employee_full_name' in row and pd.notna(row.get('employee_full_name')):
                full_name = row.get('employee_full_name')
            
            if pin and full_name and str(full_name).strip() and str(full_name).strip() != 'Unknown':
                employee_fullname_mapping[pin] = str(full_name).strip()
    
    # Add employee names to missing clock out records
    missing_clock_out_records = []
    for record in missing_clock_out_df.to_dict('records'):
        pin = record.get('employee_pin')
        # Prefer employeeFullName from the record itself (from splash page clocks - testClockingAction table)
        employee_name = None
        
        # First priority: employeeFullName directly from the record (from testClockingAction)
        # Check both field names and handle NaN values properly
        if 'employeeFullName' in record:
            full_name_val = record.get('employeeFullName')
            if full_name_val is not None and pd.notna(full_name_val) and str(full_name_val).strip() and str(full_name_val).strip() != 'Unknown':
                employee_name = str(full_name_val).strip()
        
        if not employee_name and 'employee_full_name' in record:
            full_name_val = record.get('employee_full_name')
            if full_name_val is not None and pd.notna(full_name_val) and str(full_name_val).strip() and str(full_name_val).strip() != 'Unknown':
                employee_name = str(full_name_val).strip()
        
        # Second priority: from mapping (if not found in record)
        if not employee_name:
            employee_name = resolve_employee_name(pin, employee_fullname_mapping, employee_name_mapping, record)
        
        # Set the employee_name field - only set if we have a valid name
        record['employee_name'] = employee_name if employee_name else None
        
        # Format clock_in datetime
        if 'clock_in_original' in record and record['clock_in_original']:
            record['clock_in'] = format_datetime_for_email(record['clock_in_original'])
        elif 'clock_in' in record and record['clock_in']:
            record['clock_in'] = format_datetime_for_email(record['clock_in'])
        
        missing_clock_out_records.append(record)
    
    # Add employee names and format flagged hours records
    # Flagged hours come from missing_df which is derived from clocking_df
    flagged_hours_records = []
    for record in flagged_hours_df.to_dict('records'):
        pin = record.get('employee_pin')
        
        # Get employeeFullName from clocking_df (preferred source)
        employee_name = None
        if 'clock_in' in record and record['clock_in'] and pin:
            # Try to find original record from clocking_df using normalized time
            matching_records = clocking_df[
                (clocking_df['employeePin'].astype(str) == str(pin)) & 
                (clocking_df['clock_in_normalized'].astype(str) == str(record['clock_in']))
            ]
            if len(matching_records) > 0:
                matching_record = matching_records.iloc[0]
                if 'employeeFullName' in matching_record and pd.notna(matching_record['employeeFullName']):
                    employee_name = str(matching_record['employeeFullName']).strip()
        
        # Fallback to mapping if not found in clocking_df
        if not employee_name:
            employee_name = resolve_employee_name(pin, employee_fullname_mapping, employee_name_mapping)
        
        record['employee_name'] = employee_name
        
        # Format datetime fields - find original clockIn/clockOut from clocking_df
        clock_in_formatted = 'N/A'
        clock_out_formatted = 'N/A'
        date_formatted = 'N/A'
        
        if 'clock_in' in record and record['clock_in'] and pin:
            # Try to find original clockIn from clocking_df using normalized time
            matching_records = clocking_df[
                (clocking_df['employeePin'].astype(str) == str(pin)) & 
                (clocking_df['clock_in_normalized'].astype(str) == str(record['clock_in']))
            ]
            if len(matching_records) > 0:
                clock_in_formatted = format_datetime_for_email(matching_records.iloc[0]['clockIn'])
                # Extract date part (e.g., "Jan-14, 2026" from "Jan-14, 2026 9:03 AM")
                if ',' in clock_in_formatted:
                    date_formatted = ','.join(clock_in_formatted.split(',')[:2]).strip()
                else:
                    # Fallback: extract first two parts
                    parts = clock_in_formatted.split(' ')
                    date_formatted = ' '.join(parts[:2]) if len(parts) >= 2 else clock_in_formatted
            else:
                # Fallback: try to parse the normalized datetime
                try:
                    dt = datetime.strptime(str(record['clock_in']), '%Y-%m-%d %H:%M:%S')
                    dt = dt.replace(tzinfo=ZoneInfo('UTC'))
                    pr_dt = dt.astimezone(ZoneInfo('America/Puerto_Rico'))
                    clock_in_formatted = pr_dt.strftime('%b-%d, %Y %I:%M %p')
                    # Extract date part (e.g., "Jan-14, 2026" from "Jan-14, 2026 9:03 AM")
                    if ',' in clock_in_formatted:
                        date_formatted = ','.join(clock_in_formatted.split(',')[:2]).strip()
                    else:
                        # Fallback: extract first two parts
                        parts = clock_in_formatted.split(' ')
                        date_formatted = ' '.join(parts[:2]) if len(parts) >= 2 else clock_in_formatted
                except:
                    pass
        
        if 'clock_out' in record and record['clock_out'] and pin:
            # Try to find original clockOut from clocking_df using normalized time
            matching_records = clocking_df[
                (clocking_df['employeePin'].astype(str) == str(pin)) & 
                (clocking_df['clock_out_normalized'].astype(str) == str(record['clock_out']))
            ]
            if len(matching_records) > 0:
                clock_out_formatted = format_datetime_for_email(matching_records.iloc[0]['clockOut'])
            else:
                # Fallback: try to parse the normalized datetime
                clock_out_formatted = format_normalized_datetime(record['clock_out'])
        
        record['date'] = date_formatted
        record['clock_in_time'] = clock_in_formatted
        record['clock_out_time'] = clock_out_formatted
        flagged_hours_records.append(record)
    
    # Add employee names and format orphaned records
    # Orphaned records come from timesheets_df, so use original clockDatetime/clockOutDatetime
    # But try to get employeeFullName from clocking_df if possible
    orphaned_records_list = []
    for record in orphaned_records_df.to_dict('records'):
        pin = record.get('employee_pin') or record.get('employeePin')
        
        # Prefer employeeFullName from clocking_df mapping (from splash page clocks)
        employee_name = resolve_employee_name(pin, employee_fullname_mapping, employee_name_mapping)
        record['employee_name'] = employee_name
        
        # Format datetime fields - use original clockDatetime/clockOutDatetime from timesheets
        clock_in_formatted = 'N/A'
        clock_out_formatted = 'N/A'
        date_formatted = 'N/A'
        
        # Try to get original datetimes from timesheets_df
        if 'id' in record:
            matching_timesheet = timesheets_df[timesheets_df['id'] == record['id']]
            if len(matching_timesheet) > 0:
                ts_record = matching_timesheet.iloc[0]
                if 'clockDatetime' in ts_record and pd.notna(ts_record['clockDatetime']):
                    clock_in_formatted = format_datetime_for_email(ts_record['clockDatetime'])
                    # Extract date part (e.g., "Jan-14, 2026" from "Jan-14, 2026 9:03 AM")
                    if ',' in clock_in_formatted:
                        date_formatted = ','.join(clock_in_formatted.split(',')[:2]).strip()
                    else:
                        # Fallback: extract first two parts
                        parts = clock_in_formatted.split(' ')
                        date_formatted = ' '.join(parts[:2]) if len(parts) >= 2 else clock_in_formatted
                if 'clockOutDatetime' in ts_record and pd.notna(ts_record['clockOutDatetime']):
                    clock_out_formatted = format_datetime_for_email(ts_record['clockOutDatetime'])
        
        # Fallback: try to format normalized datetime if original not found
        if clock_in_formatted == 'N/A' and 'clock_in_normalized' in record and record['clock_in_normalized']:
            clock_in_formatted = format_normalized_datetime(record['clock_in_normalized'])
            if clock_in_formatted != 'N/A':
                # Extract date part (e.g., "Jan-14, 2026" from "Jan-14, 2026 9:03 AM")
                if ',' in clock_in_formatted:
                    date_formatted = ','.join(clock_in_formatted.split(',')[:2]).strip()
                else:
                    # Fallback: extract first two parts
                    parts = clock_in_formatted.split(' ')
                    date_formatted = ' '.join(parts[:2]) if len(parts) >= 2 else clock_in_formatted
        
        if clock_out_formatted == 'N/A' and 'clock_out_normalized' in record and record['clock_out_normalized']:
            clock_out_formatted = format_normalized_datetime(record['clock_out_normalized'])
        
        record['date'] = date_formatted
        record['clock_in_time'] = clock_in_formatted
        record['clock_out_time'] = clock_out_formatted
        orphaned_records_list.append(record)
    
    # Load Jinja2 template
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env = Environment(
        loader=FileSystemLoader(script_dir),
        autoescape=select_autoescape(['html', 'xml'])
    )
    template = env.get_template('email_template.html')
    
    # Prepare data
    data = {
        'timestamp': datetime.now().strftime('%A, %B %d, %Y at %I:%M %p'),
        'missing_clock_out_count': len(missing_clock_out_df),
        'orphaned_count': len(orphaned_records_df),
        'flagged_hours_count': len(flagged_hours_df),
        'failed_uploads_count': sum(failed_reasons.values()),
        'missing_clock_out': missing_clock_out_records,
        'orphaned_records': orphaned_records_list,
        'flagged_hours': flagged_hours_records,
        'failed_reasons': failed_reasons
    }
    
    return template.render(**data)


# ============================================================================
# MAIN SCRIPT EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 80)
    print("Pet Esthetic Timesheet Sync")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # Load configuration
        config = Config.from_env()
        
        # Download Splash Page Clocks records
        clocking_df = download_test_clocking_actions(config)
        
        # Check for missing clock outs >8h
        missing_clock_out_df = check_missing_clock_out(config)
        
        # Download Timesheets records
        timesheets_df = download_timesheets(config)
        
        # Get employee PIN mapping
        employee_pin_mapping, employee_name_mapping = get_employee_pin_mapping(config)
        
        # Find missing records
        missing_df = find_missing_records(clocking_df, timesheets_df)
        
        # Validate comparison
        validation_passed, orphaned_records_df = validate_comparison(clocking_df, timesheets_df, missing_df)
        
        # Validate work hours
        missing_df, flagged_hours_df = validate_work_hours(missing_df)
        
        # Upload missing records
        created_count, failed_reasons = upload_to_timesheets(config, missing_df, employee_pin_mapping)
        
        # Summary
        print("\n" + "=" * 80)
        print("SYNC COMPLETE!")
        print("=" * 80)
        print(f"Splash Page Clocks records (valid): {len(clocking_df)}")
        print(f"Existing Timesheets records: {len(timesheets_df)}")
        print(f"Missing records found: {len(missing_df)}")
        print(f"New records created: {created_count}")
        if len(flagged_hours_df) > 0:
            print(f"Records with >8 hour shifts: {len(flagged_hours_df)}")
        if len(orphaned_records_df) > 0:
            print(f"Orphaned records in Timesheets: {len(orphaned_records_df)}")
        if len(missing_clock_out_df) > 0:
            print(f"URGENT - Missing clock out >8h: {len(missing_clock_out_df)}")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Email reporting - only if there are issues AND it's 9 AM Puerto Rico time
        has_issues = (len(orphaned_records_df) > 0 or len(flagged_hours_df) > 0 or
                     len(failed_reasons) > 0 or len(missing_clock_out_df) > 0 or not validation_passed)
        
        # Check if current time is 9 AM in Puerto Rico (allows 9:00-9:59 AM window)
        now_pr = datetime.now(ZoneInfo('America/Puerto_Rico'))
        is_email_hour = now_pr.hour == 9
        
        # Print email decision
        print("\n" + "=" * 80)
        print("EMAIL NOTIFICATION DECISION")
        print("=" * 80)
        print(f"Current time (Puerto Rico): {now_pr.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"Issues found: {'Yes' if has_issues else 'No'}")
        if has_issues:
            print(f"  - Missing clock out >8h: {len(missing_clock_out_df)}")
            print(f"  - Orphaned records: {len(orphaned_records_df)}")
            print(f"  - Long shifts (>8h): {len(flagged_hours_df)}")
            print(f"  - Failed uploads: {sum(failed_reasons.values())}")
        print(f"Email recipients configured: {'Yes' if config.email_recipients else 'No'}")
        print(f"Current hour is 9 AM: {'Yes' if is_email_hour else 'No (currently ' + str(now_pr.hour) + ':00)'}")
        
        if has_issues and config.email_recipients and is_email_hour:
            print(f"\n✓ SENDING EMAIL - All conditions met")
            print("=" * 80)
            print("\n" + "=" * 80)
            print("SENDING EMAIL REPORT (Issues Found)")
            print("=" * 80)
            
            try:
                # Generate email HTML
                email_html = generate_email_report(
                    clocking_df, timesheets_df, missing_df, created_count,
                    orphaned_records_df, flagged_hours_df, failed_reasons,
                    validation_passed, missing_clock_out_df, employee_name_mapping
                )
                
                # Create Excel file with all issues
                excel_path = create_issues_excel(
                    missing_clock_out_df, orphaned_records_df,
                    flagged_hours_df, failed_reasons
                )
                
                # Determine subject based on urgency
                if len(missing_clock_out_df) > 0:
                    subject = f"URGENTE: Alerta de Horas - Empleados Sin Marcar Salida - {datetime.now().strftime('%Y-%m-%d')}"
                elif not validation_passed:
                    subject = f"CRÍTICO: Problemas con Sistema de Horas - {datetime.now().strftime('%Y-%m-%d')}"
                else:
                    subject = f"Reporte de Horas - Problemas Detectados - {datetime.now().strftime('%Y-%m-%d')}"
                
                # Get path to logo image for inline embedding
                script_dir = os.path.dirname(os.path.abspath(__file__))
                logo_path = os.path.join(script_dir, '..', 'assets', 'pet_esthetic_transparent.png')
                logo_path = os.path.normpath(logo_path)
                
                # Prepare inline images
                inline_images = []
                if os.path.exists(logo_path):
                    inline_images = [{'path': logo_path, 'cid': 'pet-logo'}]
                
                # Send email
                send_gmail(
                    to_emails=config.email_recipients,
                    subject=subject,
                    body_html=email_html,
                    attachment_path=excel_path,
                    attachment_filename=f'timesheet_issues_{datetime.now().strftime("%Y%m%d")}.xlsx',
                    inline_images=inline_images
                )
                
                print(f"\n✓ Email successfully sent to: {', '.join(config.email_recipients)}")
                
            except Exception as e:
                print(f"\n✗ Failed to send email report: {str(e)}")
                print("  The sync completed successfully, but the email notification failed.")
        
        elif has_issues and not is_email_hour:
            print(f"\n✗ EMAIL NOT SENT - Wrong time (emails only sent at 9 AM PR time)")
            print(f"   Next email window: Tomorrow at 9:00 AM")
            print(f"   Timesheet sync completed successfully - records updated")
        elif has_issues and not config.email_recipients:
            print("\n✗ EMAIL NOT SENT - No recipients configured (EMAIL_RECIPIENTS not set)")
            print(f"   Set EMAIL_RECIPIENTS environment variable to enable email notifications")
        else:
            print("\n✓ EMAIL NOT SENT - No issues found (everything looks good!)")
        
        print("=" * 80)
        
        # Exit with appropriate code
        if len(missing_df) > 0 and created_count == 0:
            print("\n⚠️  WARNING: Records were found but none were created. Check errors above.")
            exit(1)
    
    except Exception as e:
        print("\n" + "=" * 80)
        print("ERROR!")
        print("=" * 80)
        print(f"Sync failed: {str(e)}")
        print("\nTroubleshooting tips:")
        print("- Check your NOLOCO_API_TOKEN and NOLOCO_PROJECT_ID environment variables")
        print("- Verify your internet connection")
        print("- Check if Noloco API is accessible")
        print("- Review table structures haven't changed in Noloco")
        exit(1)


if __name__ == "__main__":
    main()

import requests
import os
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import time
from tools import send_gmail

# ============================================================================
# CONFIGURATION - Update these with your credentials
# ============================================================================
# Load environment variables
#load_dotenv()
API_TOKEN = os.getenv('NOLOCO_API_TOKEN')
PROJECT_ID = os.getenv('NOLOCO_PROJECT_ID')
API_URL = f"https://api.portals.noloco.io/data/{PROJECT_ID}"

# Email configuration
EMAIL_RECIPIENTS = os.getenv('EMAIL_RECIPIENTS', '').split(',')  # Comma-separated list
EMAIL_RECIPIENTS = [email.strip() for email in EMAIL_RECIPIENTS if email.strip()]

# Add validation
if not API_TOKEN:
    raise Exception("ERROR: NOLOCO_API_TOKEN environment variable not set!")
if not PROJECT_ID:
    raise Exception("ERROR: NOLOCO_PROJECT_ID environment variable not set!")
if not EMAIL_RECIPIENTS:
    print("⚠️  WARNING: EMAIL_RECIPIENTS not set. Email reports will be disabled.")

# HTTP headers for API requests
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# Configuration for retry logic
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
RATE_LIMIT_DELAY = 0.5  # seconds between record uploads to avoid rate limiting
# ============================================================================


def normalize_datetime_for_comparison(dt_string):
    """
    Normalize any datetime string to UTC for comparison purposes.
    This removes timezone offsets, milliseconds, and converts everything to UTC time.
    
    Args:
        dt_string: String like "2025-12-16T16:51:00.000Z" or "2025-12-16T12:51:00-04:00"
        
    Returns:
        Normalized datetime string in format: "2025-12-16 16:51:00" (UTC)
    """
    if not dt_string or pd.isna(dt_string):
        return None
    
    try:
        # Remove the 'Z' at the end if present
        dt_string = str(dt_string).strip()
        
        # Parse the datetime string (handles various formats including timezone offsets)
        if dt_string.endswith('Z'):
            # UTC format: 2025-12-16T16:51:00.000Z
            clean_string = dt_string.replace('Z', '').split('.')[0]
            dt = datetime.fromisoformat(clean_string)
            dt = dt.replace(tzinfo=ZoneInfo('UTC'))
        elif '+' in dt_string or dt_string.count('-') > 2:
            # Has timezone offset: 2025-12-16T12:51:00-04:00
            dt = datetime.fromisoformat(dt_string)
            # Convert to UTC
            dt = dt.astimezone(ZoneInfo('UTC'))
        else:
            # No timezone info, assume UTC
            clean_string = dt_string.split('.')[0]
            dt = datetime.fromisoformat(clean_string)
            dt = dt.replace(tzinfo=ZoneInfo('UTC'))
        
        # Return normalized format (UTC time, no timezone suffix)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
        
    except Exception as e:
        print(f"  ⚠️  Warning: Could not normalize datetime '{dt_string}': {str(e)}")
        return None


def convert_utc_to_pr(utc_datetime_string):
    """
    Convert UTC datetime string to Puerto Rico timezone (Atlantic Standard Time)
    Puerto Rico is UTC-4 (no daylight saving time)
    
    Args:
        utc_datetime_string: String like "2025-12-16T16:51:00.000Z"
        
    Returns:
        Datetime string in Puerto Rico timezone with offset: "2025-12-16T12:51:00-04:00"
    """
    try:
        # Parse the UTC datetime
        # Remove the 'Z' at the end and the milliseconds if present
        clean_string = utc_datetime_string.replace('Z', '').split('.')[0]
        utc_dt = datetime.fromisoformat(clean_string)
        
        # Add UTC timezone info
        utc_dt = utc_dt.replace(tzinfo=ZoneInfo('UTC'))
        
        # Convert to Puerto Rico timezone (America/Puerto_Rico)
        pr_dt = utc_dt.astimezone(ZoneInfo('America/Puerto_Rico'))
        
        # Return in ISO format WITH timezone offset (Noloco needs this)
        return pr_dt.isoformat()
    except Exception as e:
        raise Exception(f"Failed to convert datetime '{utc_datetime_string}': {str(e)}")


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
                # Check for specific error types
                if "Cannot query field" in msg:
                    error_messages.append(f"Schema error: {msg} (Table structure may have changed)")
                elif "Unknown argument" in msg:
                    error_messages.append(f"Schema error: {msg} (Field name may have changed)")
                elif "Unique constraint" in msg:
                    error_messages.append(f"Duplicate record: {msg}")
                else:
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


def download_test_clocking_actions():
    """
    STEP 1: Download all records from Test Clocking Action table (Splash Page Clocks)
    STEP 2: Filter to only records with Employee Pin, Clock In, and Clock Out fully filled
    
    Returns:
        DataFrame with columns: id, employee_id, employee_pin, clock_in, clock_out, clock_in_normalized, clock_out_normalized
    """
    print("=" * 80)
    print("STEP 1: Downloading Splash Page Clocks (testClockingAction) records...")
    print("=" * 80)
    
    all_records = []
    has_more_pages = True
    cursor = None
    page_number = 1
    
    try:
        # Keep fetching pages until we have all records
        while has_more_pages:
            # Build the query
            if cursor:
                query = f"""
                query {{
                    testClockingActionCollection(first: 100, after: "{cursor}") {{
                        edges {{
                            node {{
                                id
                                employeeIdVal
                                employeePin
                                clockIn
                                clockOut
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
                                employeeIdVal
                                employeePin
                                clockIn
                                clockOut
                            }
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    }
                }
                """
            
            # Run the query
            data = run_graphql_query(query)
            collection = data.get("testClockingActionCollection", {})
            edges = collection.get("edges", [])
            page_info = collection.get("pageInfo", {})
            
            # Extract records from this page
            for edge in edges:
                node = edge.get("node", {})
                all_records.append({
                    "id": node.get("id"),
                    "employee_id": node.get("employeeIdVal"),
                    "employee_pin": node.get("employeePin"),
                    "clock_in": node.get("clockIn"),
                    "clock_out": node.get("clockOut")
                })
            
            print(f"  Downloaded page {page_number}: {len(edges)} records")
            
            # Check if there are more pages
            has_more_pages = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
            page_number += 1
        
        # Convert to DataFrame
        df = pd.DataFrame(all_records)
        
        if len(df) == 0:
            print("  ⚠️  Warning: No records found in Splash Page Clocks table")
            return df
        
        print(f"\n  Total records downloaded: {len(df)}")
        
        # STEP 2: Filter records - only keep those with ALL required fields filled
        print("\n" + "=" * 80)
        print("STEP 2: Filtering records - keeping only complete records...")
        print("=" * 80)
        
        initial_count = len(df)
        
        # Check each required field
        missing_pin = df["employee_pin"].isna() | (df["employee_pin"] == "")
        missing_clock_in = df["clock_in"].isna() | (df["clock_in"] == "")
        missing_clock_out = df["clock_out"].isna() | (df["clock_out"] == "")
        
        print(f"  Records missing Employee Pin: {missing_pin.sum()}")
        print(f"  Records missing Clock In: {missing_clock_in.sum()}")
        print(f"  Records missing Clock Out: {missing_clock_out.sum()}")
        
        # Keep only records with all three fields filled
        df = df[~(missing_pin | missing_clock_in | missing_clock_out)].copy()
        
        filtered_count = initial_count - len(df)
        print(f"\n  ✓ Filtered out {filtered_count} incomplete records")
        print(f"  ✓ Valid records remaining: {len(df)}")
        
        if len(df) == 0:
            print("  ⚠️  Warning: No valid records after filtering!")
            return df
        
        # Add normalized datetime columns for comparison
        print("\n  Normalizing datetime fields for comparison...")
        df["clock_in_normalized"] = df["clock_in"].apply(normalize_datetime_for_comparison)
        df["clock_out_normalized"] = df["clock_out"].apply(normalize_datetime_for_comparison)
        
        # Check if normalization failed for any records
        normalization_failed = df["clock_in_normalized"].isna() | df["clock_out_normalized"].isna()
        if normalization_failed.any():
            failed_count = normalization_failed.sum()
            print(f"  ⚠️  Warning: {failed_count} records failed datetime normalization (will be excluded)")
            df = df[~normalization_failed].copy()
            print(f"  ✓ Valid records after normalization: {len(df)}")
        
        return df
        
    except Exception as e:
        raise Exception(f"Failed to download Splash Page Clocks: {str(e)}")


def check_missing_clock_out(clocking_df):
    """
    Check for records missing clock out with more than 8 hours since clock in
    
    Args:
        clocking_df: DataFrame with all records from testClockingAction (before filtering)
        
    Returns:
        DataFrame with problematic records (missing clock out >8h ago)
    """
    print("\n" + "=" * 80)
    print("CHECKING FOR MISSING CLOCK OUTS (>8 hours)")
    print("=" * 80)
    
    # Get all records from the beginning (before any filtering)
    all_records = []
    has_more_pages = True
    cursor = None
    
    try:
        while has_more_pages:
            if cursor:
                query = f"""
                query {{
                    testClockingActionCollection(first: 100, after: "{cursor}") {{
                        edges {{
                            node {{
                                id
                                employeeIdVal
                                employeePin
                                clockIn
                                clockOut
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
                                employeeIdVal
                                employeePin
                                clockIn
                                clockOut
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
                all_records.append({
                    "id": node.get("id"),
                    "employee_id": node.get("employeeIdVal"),
                    "employee_pin": node.get("employeePin"),
                    "clock_in": node.get("clockIn"),
                    "clock_out": node.get("clockOut")
                })
            
            has_more_pages = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
        
        df = pd.DataFrame(all_records)
        
        if len(df) == 0:
            print("  No records to check")
            return pd.DataFrame()
        
        # Filter for records missing clock out
        missing_clock_out = df["clock_out"].isna() | (df["clock_out"] == "")
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
                # Parse clock in time
                clock_in_str = row['clock_in']
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
                        'employee_id': row['employee_id'],
                        'employee_pin': row['employee_pin'],
                        'clock_in': clock_in_pr.strftime('%Y-%m-%d %H:%M:%S'),
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
        
        return pd.DataFrame(problematic_records)
        
    except Exception as e:
        print(f"  ⚠️  Error checking for missing clock outs: {str(e)}")
        return pd.DataFrame()


def download_timesheets():
    """
    STEP 3: Download all records from Timesheets table
    STEP 4: Extract employee_pin, clockDatetime (clock in), and clockOutDatetime (clock out)
    
    Returns:
        DataFrame with columns: id, employee_pin, clock_in, clock_out, clock_in_normalized, clock_out_normalized
    """
    print("\n" + "=" * 80)
    print("STEP 3: Downloading Timesheets records...")
    print("=" * 80)
    
    all_records = []
    has_more_pages = True
    cursor = None
    page_number = 1
    
    try:
        # Keep fetching pages until we have all records
        while has_more_pages:
            # Build the query - STEP 4: Get employee_pin, clockDatetime, clockOutDatetime
            if cursor:
                query = f"""
                query {{
                    timesheetsCollection(first: 100, after: "{cursor}") {{
                        edges {{
                            node {{
                                id
                                employeePin
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
                                employeePin
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
            
            # Run the query
            data = run_graphql_query(query)
            collection = data.get("timesheetsCollection", {})
            edges = collection.get("edges", [])
            page_info = collection.get("pageInfo", {})
            
            # Extract records from this page
            for edge in edges:
                node = edge.get("node", {})
                all_records.append({
                    "id": node.get("id"),
                    "employee_pin": node.get("employeePin"),
                    "clock_in": node.get("clockDatetime"),
                    "clock_out": node.get("clockOutDatetime")
                })
            
            print(f"  Downloaded page {page_number}: {len(edges)} records")
            
            # Check if there are more pages
            has_more_pages = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
            page_number += 1
        
        # Convert to DataFrame
        df = pd.DataFrame(all_records)
        
        print(f"\n  ✓ Total records downloaded: {len(df)}")
        
        if len(df) == 0:
            print("  ⚠️  Note: Timesheets table is empty")
            return df
        
        # STEP 5: Normalize datetime fields for comparison (same format as Splash Page Clocks)
        print("\n  Normalizing datetime fields for comparison...")
        df["clock_in_normalized"] = df["clock_in"].apply(normalize_datetime_for_comparison)
        df["clock_out_normalized"] = df["clock_out"].apply(normalize_datetime_for_comparison)
        
        # Check if normalization failed for any records
        normalization_failed = df["clock_in_normalized"].isna() | df["clock_out_normalized"].isna()
        if normalization_failed.any():
            failed_count = normalization_failed.sum()
            print(f"  ⚠️  Warning: {failed_count} existing timesheet records have invalid datetime formats")
        
        return df
        
    except Exception as e:
        raise Exception(f"Failed to download Timesheets: {str(e)}")


def get_employee_pin_mapping():
    """
    Fetch all employees and create a mapping from employeePin to Employee record ID
    This is needed to create the relationship when inserting timesheet records
    
    Returns:
        Dictionary mapping employee_pin -> employee_record_id
    """
    print("\n" + "=" * 80)
    print("Fetching Employee records to map employee PINs...")
    print("=" * 80)
    
    all_employees = []
    has_more_pages = True
    cursor = None
    
    try:
        while has_more_pages:
            if cursor:
                query = f"""
                query {{
                    employeesCollection(first: 100, after: "{cursor}") {{
                        edges {{
                            node {{
                                id
                                employeePin
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
                                id
                                employeePin
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
                node = edge.get("node", {})
                if node.get("employeePin"):  # Only add if they have an employee PIN
                    all_employees.append({
                        "employee_record_id": node.get("id"),
                        "employee_pin": node.get("employeePin")
                    })
            
            has_more_pages = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
        
        # Create mapping dictionary
        mapping = {emp["employee_pin"]: emp["employee_record_id"] for emp in all_employees}
        
        if len(mapping) == 0:
            print("  ⚠️  WARNING: No employees found with PINs! All record uploads will fail.")
        else:
            print(f"  ✓ Found {len(mapping)} employees with employee PINs")
        
        return mapping
        
    except Exception as e:
        raise Exception(f"Failed to fetch Employee records: {str(e)}")


def find_missing_records(clocking_df, timesheets_df):
    """
    STEP 5: Compare records using normalized datetime fields
    Find records in Splash Page Clocks that don't exist in Timesheets
    
    We compare based on: employee_pin + clock_in_normalized + clock_out_normalized
    All datetime values are normalized to UTC format for accurate comparison
    
    Args:
        clocking_df: DataFrame from Splash Page Clocks (with normalized datetime columns)
        timesheets_df: DataFrame from Timesheets (with normalized datetime columns)
        
    Returns:
        DataFrame with missing records
    """
    print("\n" + "=" * 80)
    print("STEP 5: Comparing tables to find missing records...")
    print("=" * 80)
    
    if len(clocking_df) == 0:
        print("  No records to check from Splash Page Clocks")
        return pd.DataFrame()
    
    try:
        # Create a unique match key using NORMALIZED datetime values
        # This ensures "2025-12-16T16:51:00Z" and "2025-12-16T12:51:00-04:00" are treated as the same
        clocking_df["match_key"] = (
            clocking_df["employee_pin"].astype(str) + "_" +
            clocking_df["clock_in_normalized"].astype(str) + "_" +
            clocking_df["clock_out_normalized"].astype(str)
        )
        
        print(f"  Splash Page Clocks records to check: {len(clocking_df)}")
        
        if len(timesheets_df) > 0:
            timesheets_df["match_key"] = (
                timesheets_df["employee_pin"].astype(str) + "_" +
                timesheets_df["clock_in_normalized"].astype(str) + "_" +
                timesheets_df["clock_out_normalized"].astype(str)
            )
            
            print(f"  Existing Timesheets records: {len(timesheets_df)}")
            
            # Early warning if Timesheets has more records than source
            if len(timesheets_df) > len(clocking_df):
                print(f"\n  ⚠️  WARNING: Timesheets has MORE records than Splash Page Clocks!")
                print(f"              This indicates potential data integrity issues.")
                print(f"              See validation step for details.")
            
            # Show sample of what we're comparing
            print("\n  Sample comparison keys:")
            print("  From Splash Page Clocks:")
            for key in clocking_df["match_key"].head(3):
                print(f"    {key}")
            print("  From Timesheets:")
            for key in timesheets_df["match_key"].head(3):
                print(f"    {key}")
            
            # Find records in clocking that are NOT in timesheets
            missing_records = clocking_df[~clocking_df["match_key"].isin(timesheets_df["match_key"])].copy()
        else:
            # If timesheets is empty, all clocking records are missing
            print("  Existing Timesheets records: 0 (table is empty)")
            missing_records = clocking_df.copy()
        
        # Remove the match_key column (we don't need it anymore)
        missing_records = missing_records.drop(columns=["match_key"])
        
        print(f"\n  ✓ Missing records found: {len(missing_records)}")
        
        if len(missing_records) > 0:
            print("\n  Preview of missing records:")
            preview = missing_records[["employee_pin", "clock_in", "clock_out", "clock_in_normalized", "clock_out_normalized"]].head(5)
            print(preview.to_string(index=False))
        
        return missing_records
        
    except Exception as e:
        raise Exception(f"Failed to compare records: {str(e)}")


def validate_comparison(clocking_df, timesheets_df, missing_df):
    """
    STEP 6: Post-comparison validation
    Verify the comparison logic is working correctly
    
    Args:
        clocking_df: Original Splash Page Clocks DataFrame
        timesheets_df: Original Timesheets DataFrame
        missing_df: Missing records DataFrame
        
    Returns:
        Tuple of (validation_passed: bool, orphaned_records_df: DataFrame)
    """
    print("\n" + "=" * 80)
    print("STEP 6: POST-COMPARISON VALIDATION")
    print("=" * 80)
    
    validation_passed = True
    orphaned_records_df = pd.DataFrame()
    
    # Validation 0: Check that Timesheets doesn't have more records than source
    if len(timesheets_df) > len(clocking_df):
        print(f"  ✗ FAIL: Timesheets has MORE records ({len(timesheets_df)}) than Splash Page Clocks ({len(clocking_df)})!")
        print(f"         This indicates data integrity issues:")
        print(f"         - Records may have been added manually to Timesheets")
        print(f"         - Duplicate records exist in Timesheets")
        print(f"         - Records may have been deleted from Splash Page Clocks")
        validation_passed = False
        
        # Find orphaned records
        if len(clocking_df) > 0:
            clocking_keys = set(
                clocking_df["employee_pin"].astype(str) + "_" +
                clocking_df["clock_in_normalized"].astype(str) + "_" +
                clocking_df["clock_out_normalized"].astype(str)
            )
            
            timesheets_df["match_key_temp"] = (
                timesheets_df["employee_pin"].astype(str) + "_" +
                timesheets_df["clock_in_normalized"].astype(str) + "_" +
                timesheets_df["clock_out_normalized"].astype(str)
            )
            
            orphaned_records_df = timesheets_df[~timesheets_df["match_key_temp"].isin(clocking_keys)].copy()
            orphaned_records_df = orphaned_records_df.drop(columns=["match_key_temp"])
            
            if len(orphaned_records_df) > 0:
                print(f"         Found {len(orphaned_records_df)} records in Timesheets that don't exist in Splash Page Clocks")
                print(f"\n         ORPHANED TIMESHEET RECORDS TO INVESTIGATE:")
                print(f"         {'ID':<25} {'Employee PIN':<15} {'Clock In':<25} {'Clock Out':<25}")
                print(f"         {'-'*90}")
                
                for idx, row in orphaned_records_df.head(10).iterrows():
                    timesheet_id = row.get('id', 'N/A')
                    emp_pin = row.get('employee_pin', 'N/A')
                    clock_in = row.get('clock_in_normalized', 'N/A')
                    clock_out = row.get('clock_out_normalized', 'N/A')
                    print(f"         {timesheet_id:<25} {emp_pin:<15} {clock_in:<25} {clock_out:<25}")
                
                if len(orphaned_records_df) > 10:
                    print(f"         ... and {len(orphaned_records_df) - 10} more orphaned records")
                
                print(f"\n         You can search for these IDs in your Noloco Timesheets table to investigate.")
    else:
        print(f"  ✓ PASS: Timesheets records ({len(timesheets_df)}) ≤ Source records ({len(clocking_df)})")
    
    # Validation 1: Check that missing_df is a subset of clocking_df
    if len(missing_df) > len(clocking_df):
        print("  ✗ FAIL: Missing records count exceeds source records count!")
        validation_passed = False
    else:
        print(f"  ✓ PASS: Missing records ({len(missing_df)}) ≤ Source records ({len(clocking_df)})")
    
    # Validation 2: Verify normalized datetime fields exist and are valid
    if len(missing_df) > 0:
        invalid_normalized = missing_df["clock_in_normalized"].isna() | missing_df["clock_out_normalized"].isna()
        if invalid_normalized.any():
            print(f"  ✗ FAIL: {invalid_normalized.sum()} missing records have invalid normalized datetime!")
            validation_passed = False
        else:
            print(f"  ✓ PASS: All missing records have valid normalized datetime fields")
    
    # Validation 3: Manual spot check
    if len(missing_df) > 0 and len(timesheets_df) > 0:
        print("\n  Manual spot check - verifying 3 random missing records:")
        sample_size = min(3, len(missing_df))
        sample = missing_df.sample(n=sample_size)
        
        for idx, row in sample.iterrows():
            exists = timesheets_df[
                (timesheets_df["employee_pin"] == row["employee_pin"]) &
                (timesheets_df["clock_in_normalized"] == row["clock_in_normalized"]) &
                (timesheets_df["clock_out_normalized"] == row["clock_out_normalized"])
            ]
            
            if len(exists) > 0:
                print(f"    ✗ FAIL: Record should NOT be missing - PIN: {row['employee_pin']}, Clock In: {row['clock_in_normalized']}")
                validation_passed = False
            else:
                print(f"    ✓ PASS: Record is truly missing - PIN: {row['employee_pin']}, Clock In: {row['clock_in_normalized']}")
    
    # Validation 4: Check for duplicates
    if len(missing_df) > 0:
        match_key_check = (
            missing_df["employee_pin"].astype(str) + "_" +
            missing_df["clock_in_normalized"].astype(str) + "_" +
            missing_df["clock_out_normalized"].astype(str)
        )
        duplicates = match_key_check.duplicated()
        if duplicates.any():
            print(f"  ✗ FAIL: {duplicates.sum()} duplicate records found in missing records!")
            validation_passed = False
        else:
            print(f"  ✓ PASS: No duplicate records in missing records")
    
    # Summary statistics
    print("\n  Summary Statistics:")
    print(f"    Total Splash Page Clocks records: {len(clocking_df)}")
    print(f"    Total Timesheets records: {len(timesheets_df)}")
    print(f"    Missing records to add: {len(missing_df)}")
    if len(clocking_df) > 0:
        match_percentage = ((len(clocking_df) - len(missing_df)) / len(clocking_df)) * 100
        print(f"    Match rate: {match_percentage:.1f}%")
    
    # Final validation result
    print("\n" + "=" * 80)
    if validation_passed:
        print("  ✓✓✓ VALIDATION PASSED - Comparison logic is working correctly")
    else:
        print("  ✗✗✗ VALIDATION FAILED - Review errors above!")
    print("=" * 80)
    
    return validation_passed, orphaned_records_df


def validate_work_hours(records_df):
    """
    Validate that work shifts are reasonable (not more than 8 hours)
    Flags records with shifts longer than 8 hours for review
    
    Args:
        records_df: DataFrame with records to validate
        
    Returns:
        Tuple of (valid_records_df, flagged_records_df)
    """
    if len(records_df) == 0:
        return records_df, pd.DataFrame()
    
    print("\n" + "=" * 80)
    print("WORK HOURS VALIDATION")
    print("=" * 80)
    
    flagged_records = []
    
    for idx, row in records_df.iterrows():
        try:
            # Parse the clock in and clock out times
            clock_in_str = row['clock_in']
            clock_out_str = row['clock_out']
            
            # Parse datetimes (handle both UTC and timezone formats)
            if clock_in_str.endswith('Z'):
                clock_in_clean = clock_in_str.replace('Z', '').split('.')[0]
                clock_in_dt = datetime.fromisoformat(clock_in_clean).replace(tzinfo=ZoneInfo('UTC'))
            else:
                clock_in_dt = datetime.fromisoformat(clock_in_str)
            
            if clock_out_str.endswith('Z'):
                clock_out_clean = clock_out_str.replace('Z', '').split('.')[0]
                clock_out_dt = datetime.fromisoformat(clock_out_clean).replace(tzinfo=ZoneInfo('UTC'))
            else:
                clock_out_dt = datetime.fromisoformat(clock_out_str)
            
            # Calculate hours worked
            time_diff = clock_out_dt - clock_in_dt
            hours_worked = time_diff.total_seconds() / 3600
            
            # Flag if more than 8 hours
            if hours_worked > 8:
                flagged_records.append({
                    'index': idx,
                    'employee_pin': row['employee_pin'],
                    'clock_in': row['clock_in_normalized'],
                    'clock_out': row['clock_out_normalized'],
                    'hours_worked': round(hours_worked, 2),
                    'timesheet_id': row.get('id', 'N/A')
                })
        
        except Exception as e:
            print(f"  ⚠️  Warning: Could not calculate hours for record {idx}: {str(e)}")
    
    # Report flagged records
    if len(flagged_records) > 0:
        print(f"\n  ⚠️  WARNING: Found {len(flagged_records)} records with shifts LONGER than 8 hours!")
        print(f"\n  FLAGGED RECORDS (>8 hours):")
        print(f"  {'Employee PIN':<15} {'Clock In':<25} {'Clock Out':<25} {'Hours':<10}")
        print(f"  {'-'*75}")
        
        for record in flagged_records[:20]:
            print(f"  {record['employee_pin']:<15} {record['clock_in']:<25} {record['clock_out']:<25} {record['hours_worked']:<10.2f}")
        
        if len(flagged_records) > 20:
            print(f"  ... and {len(flagged_records) - 20} more flagged records")
        
        print(f"\n  These records may indicate:")
        print(f"  - Employees forgot to clock out (worked overnight?)")
        print(f"  - Data entry errors")
        print(f"  - Legitimate long shifts that need review")
        print(f"\n  ❓ The script will upload ALL records including those >8 hours.")
        print(f"     Review these records manually in Noloco after upload if needed.")
    else:
        print(f"  ✓ PASS: All records have work shifts ≤ 8 hours")
    
    # Create a flagged records DataFrame
    flagged_df = pd.DataFrame(flagged_records) if flagged_records else pd.DataFrame()
    
    return records_df, flagged_df


def generate_email_report(
    clocking_df,
    timesheets_df,
    missing_df,
    created_count,
    orphaned_records_df,
    flagged_hours_df,
    failed_reasons,
    validation_passed,
    missing_clock_out_df
):
    """
    Generate an HTML email report summarizing all issues found during sync
    """
    
    # Determine overall status
    has_issues = (
        len(orphaned_records_df) > 0 or
        len(flagged_hours_df) > 0 or
        len(failed_reasons) > 0 or
        len(missing_clock_out_df) > 0 or
        not validation_passed
    )
    
    if not validation_passed:
        status_color = "#dc3545"
        status_text = "⚠️ CRITICAL ISSUES FOUND"
    elif has_issues:
        status_color = "#ffc107"
        status_text = "⚠️ COMPLETED WITH WARNINGS"
    else:
        status_color = "#28a745"
        status_text = "✓ COMPLETED SUCCESSFULLY"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 900px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f5f5f5;
            }}
            .container {{
                background-color: white;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .header {{
                background-color: {status_color};
                color: white;
                padding: 25px;
                border-radius: 8px;
                margin-bottom: 25px;
            }}
            .header h1 {{
                margin: 0 0 10px 0;
                font-size: 28px;
            }}
            .header h2 {{
                margin: 0 0 10px 0;
                font-size: 20px;
            }}
            .summary {{
                background-color: #f8f9fa;
                padding: 20px;
                border-radius: 8px;
                margin-bottom: 25px;
                border-left: 4px solid {status_color};
            }}
            .summary h3 {{
                margin-top: 0;
                color: #333;
            }}
            .summary-item {{
                display: flex;
                justify-content: space-between;
                padding: 10px 0;
                border-bottom: 1px solid #dee2e6;
            }}
            .summary-item:last-child {{
                border-bottom: none;
            }}
            .summary-label {{
                font-weight: bold;
                color: #555;
            }}
            .summary-value {{
                font-weight: bold;
                color: #000;
            }}
            .section {{
                margin-bottom: 30px;
            }}
            .section-title {{
                background-color: #e9ecef;
                padding: 12px 15px;
                border-radius: 5px;
                font-weight: bold;
                margin-bottom: 15px;
                font-size: 16px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 10px;
                font-size: 14px;
            }}
            th, td {{
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #dee2e6;
            }}
            th {{
                background-color: #e9ecef;
                font-weight: bold;
                color: #333;
            }}
            tr:hover {{
                background-color: #f8f9fa;
            }}
            .warning-box {{
                background-color: #fff3cd;
                border-left: 4px solid #ffc107;
                padding: 15px;
                margin: 15px 0;
                border-radius: 4px;
            }}
            .error-box {{
                background-color: #f8d7da;
                border-left: 4px solid #dc3545;
                padding: 15px;
                margin: 15px 0;
                border-radius: 4px;
            }}
            .success-box {{
                background-color: #d4edda;
                border-left: 4px solid #28a745;
                padding: 15px;
                margin: 15px 0;
                border-radius: 4px;
            }}
            .issue-count {{
                background-color: #dc3545;
                color: white;
                padding: 2px 8px;
                border-radius: 12px;
                font-size: 12px;
                font-weight: bold;
                margin-left: 10px;
            }}
            .footer {{
                margin-top: 40px;
                padding-top: 20px;
                border-top: 2px solid #dee2e6;
                font-size: 13px;
                color: #6c757d;
                text-align: center;
            }}
            ul {{
                margin: 10px 0;
                padding-left: 20px;
            }}
            li {{
                margin: 5px 0;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🕒 Pet Esthetic Timesheet Sync Report</h1>
                <h2>{status_text}</h2>
                <p style="margin: 5px 0;">Generated: {datetime.now().strftime('%A, %B %d, %Y at %I:%M %p')}</p>
            </div>
            
            <div class="summary">
                <h3>📊 Sync Summary</h3>
                <div class="summary-item">
                    <span class="summary-label">Splash Page Clocks (valid records):</span>
                    <span class="summary-value">{len(clocking_df)}</span>
                </div>
                <div class="summary-item">
                    <span class="summary-label">Existing Timesheets records:</span>
                    <span class="summary-value">{len(timesheets_df)}</span>
                </div>
                <div class="summary-item">
                    <span class="summary-label">Missing records found:</span>
                    <span class="summary-value">{len(missing_df)}</span>
                </div>
                <div class="summary-item">
                    <span class="summary-label">New records created:</span>
                    <span class="summary-value" style="color: #28a745;">{created_count}</span>
                </div>
            </div>
    """
    
    # Issues overview
    total_issues = (len(orphaned_records_df) + len(flagged_hours_df) + 
                   sum(failed_reasons.values()) + len(missing_clock_out_df))
    
    if total_issues > 0:
        html += f"""
            <div class="section">
                <div class="section-title">⚠️ Issues Overview</div>
                <div class="warning-box">
                    <strong style="font-size: 16px;">Total Issues Found: {total_issues}</strong>
                    <ul style="margin-top: 10px;">
        """
        
        if len(missing_clock_out_df) > 0:
            html += f"<li><strong>🚨 Missing Clock Out >8h:</strong> {len(missing_clock_out_df)} (URGENT - employees may still be clocked in!)</li>"
        if len(orphaned_records_df) > 0:
            html += f"<li><strong>Orphaned Records:</strong> {len(orphaned_records_df)} (in Timesheets but not in source)</li>"
        if len(flagged_hours_df) > 0:
            html += f"<li><strong>Long Shifts:</strong> {len(flagged_hours_df)} (work shifts > 8 hours)</li>"
        if len(failed_reasons) > 0:
            total_failed = sum(failed_reasons.values())
            html += f"<li><strong>Failed Uploads:</strong> {total_failed}</li>"
        
        html += """
                    </ul>
                </div>
            </div>
        """
    
    # URGENT ISSUE: Missing Clock Out >8h
    if len(missing_clock_out_df) > 0:
        html += f"""
            <div class="section">
                <div class="section-title">
                    🚨 URGENT: Missing Clock Out >8 Hours
                    <span class="issue-count">{len(missing_clock_out_df)}</span>
                </div>
                <div class="error-box">
                    <strong>⚠️ IMMEDIATE ACTION REQUIRED!</strong>
                    <p style="margin: 10px 0 5px 0;">These employees clocked in more than 8 hours ago but never clocked out:</p>
                    <ul style="margin-top: 5px;">
                        <li>They may still be working (forgot to clock out)</li>
                        <li>System/app error prevented clock out</li>
                        <li>Employee emergency or incident</li>
                    </ul>
                    <p style="margin-top: 10px;"><strong>Action Required:</strong> Contact these employees immediately to verify their status!</p>
                </div>
                <table>
                    <tr>
                        <th>Employee PIN</th>
                        <th>Clock In (PR Time)</th>
                        <th>Hours Since Clock In</th>
                    </tr>
        """
        
        for idx, row in missing_clock_out_df.head(50).iterrows():
            hours = row.get('hours_since_clock_in', 0)
            if hours > 24:
                hours_color = "#dc3545"  # Red - very urgent
            elif hours > 16:
                hours_color = "#fd7e14"  # Orange - urgent
            else:
                hours_color = "#ffc107"  # Yellow - warning
                
            html += f"""
                    <tr>
                        <td>{row.get('employee_pin', 'N/A')}</td>
                        <td>{row.get('clock_in', 'N/A')}</td>
                        <td style="font-weight: bold; color: {hours_color};">{hours:.1f}</td>
                    </tr>
            """
        
        if len(missing_clock_out_df) > 50:
            html += f"""
                    <tr>
                        <td colspan="3" style="text-align: center; font-style: italic; background-color: #f8d7da;">
                            ⚠️ {len(missing_clock_out_df) - 50} more records (see attached CSV)
                        </td>
                    </tr>
            """
        
        html += """
                </table>
            </div>
        """
    
    # ISSUE: Orphaned Records
    if len(orphaned_records_df) > 0:
        html += f"""
            <div class="section">
                <div class="section-title">
                    🔴 Orphaned Records 
                    <span class="issue-count">{len(orphaned_records_df)}</span>
                </div>
                <div class="error-box">
                    <strong>⚠️ These records exist in Timesheets but NOT in Splash Page Clocks!</strong>
                    <p style="margin: 10px 0 5px 0;">This may indicate:</p>
                    <ul style="margin-top: 5px;">
                        <li>Records were added manually to Timesheets</li>
                        <li>Duplicate records exist in Timesheets</li>
                        <li>Records were deleted from Splash Page Clocks</li>
                    </ul>
                    <p style="margin-top: 10px;"><strong>Action Required:</strong> Review these records in Noloco and delete if they are duplicates or errors.</p>
                </div>
                <table>
                    <tr>
                        <th>Timesheet ID</th>
                        <th>Employee PIN</th>
                        <th>Clock In</th>
                        <th>Clock Out</th>
                    </tr>
        """
        
        for idx, row in orphaned_records_df.head(50).iterrows():
            html += f"""
                    <tr>
                        <td><code>{row.get('id', 'N/A')}</code></td>
                        <td>{row.get('employee_pin', 'N/A')}</td>
                        <td>{row.get('clock_in_normalized', 'N/A')}</td>
                        <td>{row.get('clock_out_normalized', 'N/A')}</td>
                    </tr>
            """
        
        if len(orphaned_records_df) > 50:
            html += f"""
                    <tr>
                        <td colspan="4" style="text-align: center; font-style: italic; background-color: #fff3cd;">
                            ⚠️ {len(orphaned_records_df) - 50} more records (see attached CSV)
                        </td>
                    </tr>
            """
        
        html += """
                </table>
            </div>
        """
    
    # ISSUE: Long work shifts
    if len(flagged_hours_df) > 0:
        html += f"""
            <div class="section">
                <div class="section-title">
                    🟡 Long Work Shifts (>8 hours)
                    <span class="issue-count">{len(flagged_hours_df)}</span>
                </div>
                <div class="warning-box">
                    <strong>⚠️ These records have work shifts longer than 8 hours</strong>
                    <p style="margin: 10px 0 5px 0;">This may indicate:</p>
                    <ul style="margin-top: 5px;">
                        <li>Employees forgot to clock out (worked overnight?)</li>
                        <li>Data entry errors in clock in/out times</li>
                        <li>Legitimate long shifts (overtime, special events)</li>
                    </ul>
                    <p style="margin-top: 10px;"><strong>Action Required:</strong> Verify these shifts with employees and correct any errors.</p>
                </div>
                <table>
                    <tr>
                        <th>Employee PIN</th>
                        <th>Clock In</th>
                        <th>Clock Out</th>
                        <th>Hours Worked</th>
                    </tr>
        """
        
        for record in flagged_hours_df.head(50).to_dict('records'):
            hours = record.get('hours_worked', 0)
            if hours > 16:
                hours_color = "#dc3545"
            elif hours > 12:
                hours_color = "#fd7e14"
            else:
                hours_color = "#ffc107"
                
            html += f"""
                    <tr>
                        <td>{record.get('employee_pin', 'N/A')}</td>
                        <td>{record.get('clock_in', 'N/A')}</td>
                        <td>{record.get('clock_out', 'N/A')}</td>
                        <td style="font-weight: bold; color: {hours_color};">{hours:.2f}</td>
                    </tr>
            """
        
        if len(flagged_hours_df) > 50:
            html += f"""
                    <tr>
                        <td colspan="4" style="text-align: center; font-style: italic; background-color: #fff3cd;">
                            ⚠️ {len(flagged_hours_df) - 50} more records (see attached CSV)
                        </td>
                    </tr>
            """
        
        html += """
                </table>
            </div>
        """
    
    # ISSUE: Failed Uploads
    if len(failed_reasons) > 0:
        total_failed = sum(failed_reasons.values())
        html += f"""
            <div class="section">
                <div class="section-title">
                    🔴 Failed Uploads
                    <span class="issue-count">{total_failed}</span>
                </div>
                <div class="error-box">
                    <strong>⚠️ These records failed to upload to Timesheets</strong>
                    <p style="margin-top: 10px;"><strong>Action Required:</strong> Review the failure reasons below and fix the underlying issues.</p>
                </div>
                <table>
                    <tr>
                        <th>Failure Reason</th>
                        <th>Count</th>
                    </tr>
        """
        
        for reason, count in failed_reasons.items():
            html += f"""
                    <tr>
                        <td>{reason}</td>
                        <td style="font-weight: bold; color: #dc3545;">{count}</td>
                    </tr>
            """
        
        html += """
                </table>
            </div>
        """
    
    # Footer
    html += f"""
            <div class="footer">
                <p><strong>Pet Esthetic Timesheet Sync</strong></p>
                <p>Automated sync between Splash Page Clocks and Timesheets tables</p>
                <p style="font-size: 12px; margin-top: 10px;">
                    This report was automatically generated by the Noloco Timesheet Sync script.<br>
                    For questions or issues, please contact your system administrator.
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html


def upload_to_timesheets(records_df, employee_pin_mapping):
    """
    Upload new records to Timesheets table
    
    Args:
        records_df: DataFrame with records to upload
        employee_pin_mapping: Dictionary mapping employee_pin to employee_record_id
        
    Returns:
        Tuple of (created_count, failed_reasons_dict)
    """
    if len(records_df) == 0:
        print("\n" + "=" * 80)
        print("STEP 7: No records to upload")
        print("=" * 80)
        return 0, {}
    
    print("\n" + "=" * 80)
    print(f"STEP 7: Uploading {len(records_df)} records to Timesheets...")
    print("=" * 80)
    
    created_count = 0
    failed_count = 0
    failed_reasons = {}
    
    for index, row in records_df.iterrows():
        try:
            # Convert UTC times to Puerto Rico timezone for storage
            clock_in_pr = convert_utc_to_pr(row['clock_in'])
            clock_out_pr = convert_utc_to_pr(row['clock_out'])
            
            # Extract date and create midnight timestamp
            date_only = clock_in_pr.split('T')[0]
            timesheet_date = f"{date_only}T00:00:00-04:00"
            
            # Get employee record ID
            employee_record_id = employee_pin_mapping.get(row['employee_pin'])
            
            if not employee_record_id:
                reason = f"No employee found for PIN {row['employee_pin']}"
                print(f"  ⚠️  Skipping record {index + 1}: {reason}")
                failed_count += 1
                failed_reasons[reason] = failed_reasons.get(reason, 0) + 1
                continue
            
            # Create the timesheet record
            create_mutation = f"""
            mutation {{
                createTimesheets(
                    employeeIdVal: "{row['employee_id']}",
                    employeePin: "{row['employee_pin']}",
                    clockDatetime: "{clock_in_pr}",
                    clockOutDatetime: "{clock_out_pr}",
                    timesheetDate: "{timesheet_date}",
                    relatedEmployeeId: "{employee_record_id}"
                ) {{
                    id
                }}
            }}
            """
            
            # Execute the mutation
            result = run_graphql_query(create_mutation)
            timesheet_id = result.get("createTimesheets", {}).get("id")
            created_count += 1
            print(f"  ✓ Created record {created_count}/{len(records_df)}: Employee PIN {row['employee_pin']} | {row['clock_in_normalized']}")
            
            # Rate limiting delay
            if RATE_LIMIT_DELAY > 0 and created_count < len(records_df):
                time.sleep(RATE_LIMIT_DELAY)
                
        except Exception as e:
            failed_count += 1
            error_msg = str(e)
            
            if "Schema error" in error_msg:
                reason = "Schema error (table structure changed)"
            elif "Duplicate record" in error_msg:
                reason = "Duplicate record"
            elif "Rate limit" in error_msg:
                reason = "Rate limit exceeded"
            else:
                reason = "API error"
            
            print(f"  ✗ Failed record {index + 1}: {error_msg}")
            failed_reasons[reason] = failed_reasons.get(reason, 0) + 1
    
    # Summary
    print("\n  Upload Summary:")
    print(f"  ✓ Successfully created: {created_count}")
    if failed_count > 0:
        print(f"  ✗ Failed: {failed_count}")
        print("\n  Failure breakdown:")
        for reason, count in failed_reasons.items():
            print(f"    - {reason}: {count}")
    
    return created_count, failed_reasons


# ============================================================================
# MAIN SCRIPT EXECUTION
# ============================================================================

print("=" * 80)
print("Pet Esthetic Timesheet Sync")
print("=" * 80)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

try:
    # STEP 1 & 2: Download and filter Splash Page Clocks records
    clocking_df = download_test_clocking_actions()
    
    # CHECK: Missing clock outs >8h (URGENT)
    missing_clock_out_df = check_missing_clock_out(clocking_df)
    
    # STEP 3 & 4: Download Timesheets records
    timesheets_df = download_timesheets()
    
    # Get employee PIN mapping
    employee_pin_mapping = get_employee_pin_mapping()
    
    # STEP 5: Find missing records
    missing_df = find_missing_records(clocking_df, timesheets_df)
    
    # STEP 6: Validate comparison
    validation_passed, orphaned_records_df = validate_comparison(clocking_df, timesheets_df, missing_df)
    
    if not validation_passed:
        print("\n⚠️  WARNING: Validation failed! Review errors before uploading.")
    
    # WORK HOURS VALIDATION
    missing_df, flagged_hours_df = validate_work_hours(missing_df)
    
    # STEP 7: Upload missing records
    created_count, failed_reasons = upload_to_timesheets(missing_df, employee_pin_mapping)
    
    # Final Summary
    print("\n" + "=" * 80)
    print("SYNC COMPLETE!")
    print("=" * 80)
    print(f"Splash Page Clocks records (valid): {len(clocking_df)}")
    print(f"Existing Timesheets records: {len(timesheets_df)}")
    print(f"Missing records found: {len(missing_df)}")
    print(f"New records created: {created_count}")
    if len(flagged_hours_df) > 0:
        print(f"⚠️  Records with >8 hour shifts: {len(flagged_hours_df)}")
    if len(orphaned_records_df) > 0:
        print(f"⚠️  Orphaned records in Timesheets: {len(orphaned_records_df)}")
    if len(missing_clock_out_df) > 0:
        print(f"🚨 URGENT - Missing clock out >8h: {len(missing_clock_out_df)}")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # ========================================================================
    # EMAIL REPORTING - Only if there are issues
    # ========================================================================
    has_issues = (
        len(orphaned_records_df) > 0 or
        len(flagged_hours_df) > 0 or
        len(failed_reasons) > 0 or
        len(missing_clock_out_df) > 0 or
        not validation_passed
    )
    
    if has_issues and EMAIL_RECIPIENTS:
        print("\n" + "=" * 80)
        print("SENDING EMAIL REPORT (Issues Found)")
        print("=" * 80)
        
        try:
            # Generate email report
            email_html = generate_email_report(
                clocking_df=clocking_df,
                timesheets_df=timesheets_df,
                missing_df=missing_df,
                created_count=created_count,
                orphaned_records_df=orphaned_records_df,
                flagged_hours_df=flagged_hours_df,
                failed_reasons=failed_reasons,
                validation_passed=validation_passed,
                missing_clock_out_df=missing_clock_out_df
            )
            
            # Prepare CSV attachments for issues (if any)
            attachment_df = None
            attachment_name = None
            
            if len(missing_clock_out_df) > 0:
                # Prioritize missing clock out as attachment (most urgent)
                attachment_df = missing_clock_out_df
                attachment_name = f"missing_clock_out_{datetime.now().strftime('%Y%m%d')}.csv"
            elif len(orphaned_records_df) > 0:
                attachment_df = orphaned_records_df
                attachment_name = f"orphaned_records_{datetime.now().strftime('%Y%m%d')}.csv"
            elif len(flagged_hours_df) > 0:
                attachment_df = flagged_hours_df
                attachment_name = f"flagged_hours_{datetime.now().strftime('%Y%m%d')}.csv"
            
            # Determine subject based on urgency
            if len(missing_clock_out_df) > 0:
                subject = f"🚨 URGENT: Timesheet Sync Alert - Missing Clock Outs - {datetime.now().strftime('%Y-%m-%d')}"
            elif not validation_passed:
                subject = f"⚠️ CRITICAL: Timesheet Sync Issues - {datetime.now().strftime('%Y-%m-%d')}"
            else:
                subject = f"⚠️ Timesheet Sync Report - Issues Found - {datetime.now().strftime('%Y-%m-%d')}"
            
            # Send email
            send_gmail(
                to_emails=EMAIL_RECIPIENTS,
                subject=subject,
                body_html=email_html,
                df_attachment=attachment_df,
                attachment_filename=attachment_name
            )
            
            print(f"✓ Email report sent to: {', '.join(EMAIL_RECIPIENTS)}")
            
        except Exception as e:
            print(f"✗ Failed to send email report: {str(e)}")
            print("  The sync completed successfully, but the email notification failed.")
    
    elif has_issues and not EMAIL_RECIPIENTS:
        print("\n⚠️  Issues found but EMAIL_RECIPIENTS not configured. No email sent.")
    else:
        print("\n✓ No issues found - email report not needed.")
    
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

import requests
import os
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import time

# ============================================================================
# CONFIGURATION - Update these with your credentials
# ============================================================================
# Load environment variables
#load_dotenv()
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
    """
    print("\n" + "=" * 80)
    print("STEP 6: POST-COMPARISON VALIDATION")
    print("=" * 80)
    
    validation_passed = True
    
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
    
    # Validation 3: Manual spot check - verify a few records are truly missing
    if len(missing_df) > 0 and len(timesheets_df) > 0:
        print("\n  Manual spot check - verifying 3 random missing records:")
        sample_size = min(3, len(missing_df))
        sample = missing_df.sample(n=sample_size)
        
        for idx, row in sample.iterrows():
            # Check if this record actually exists in timesheets
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
    
    # Validation 4: Check for duplicate match keys in missing records
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
    
    # Validation 5: Summary statistics
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
    
    return validation_passed


def upload_to_timesheets(records_df, employee_pin_mapping):
    """
    Upload new records to Timesheets table
    
    Args:
        records_df: DataFrame with records to upload
        employee_pin_mapping: Dictionary mapping employee_pin to employee_record_id
        
    Returns:
        Number of successfully created records
    """
    if len(records_df) == 0:
        print("\n" + "=" * 80)
        print("STEP 7: No records to upload")
        print("=" * 80)
        return 0
    
    print("\n" + "=" * 80)
    print(f"STEP 7: Uploading {len(records_df)} records to Timesheets...")
    print("=" * 80)
    
    created_count = 0
    failed_count = 0
    failed_reasons = {}  # Track failure reasons for summary
    
    # Upload each record one by one
    for index, row in records_df.iterrows():
        try:
            # Convert UTC times to Puerto Rico timezone for storage
            clock_in_pr = convert_utc_to_pr(row['clock_in'])
            clock_out_pr = convert_utc_to_pr(row['clock_out'])
            
            # Extract just the date and create a datetime at midnight Puerto Rico time
            date_only = clock_in_pr.split('T')[0]  # Gets "2025-12-16"
            timesheet_date = f"{date_only}T00:00:00-04:00"  # Midnight PR time with timezone
            
            # Get the employee record ID using the employee PIN
            employee_record_id = employee_pin_mapping.get(row['employee_pin'])
            
            if not employee_record_id:
                reason = f"No employee found for PIN {row['employee_pin']}"
                print(f"  ⚠️  Skipping record {index + 1}: {reason}")
                failed_count += 1
                failed_reasons[reason] = failed_reasons.get(reason, 0) + 1
                continue
            
            # Create the timesheet record with the employee link using relatedEmployeeId
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
            
            # Small delay to avoid rate limiting
            if RATE_LIMIT_DELAY > 0 and created_count < len(records_df):
                time.sleep(RATE_LIMIT_DELAY)
                
        except Exception as e:
            failed_count += 1
            error_msg = str(e)
            # Simplify error message for common cases
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
    
    return created_count


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
    
    # STEP 3 & 4: Download Timesheets records with correct fields
    timesheets_df = download_timesheets()
    
    # Get employee PIN mapping (needed for upload step)
    employee_pin_mapping = get_employee_pin_mapping()
    
    # STEP 5: Find missing records using normalized datetime comparison
    missing_df = find_missing_records(clocking_df, timesheets_df)
    
    # STEP 6: Validate the comparison
    validation_passed = validate_comparison(clocking_df, timesheets_df, missing_df)
    
    if not validation_passed:
        print("\n⚠️  WARNING: Validation failed! Review errors before uploading.")
        print("Proceeding with upload anyway... (remove this line to stop on validation failure)")
        # Uncomment the next line to stop if validation fails:
        # raise Exception("Validation failed - stopping before upload")
    
    # STEP 7: Upload missing records
    created_count = upload_to_timesheets(missing_df, employee_pin_mapping)
    
    # Final Summary
    print("\n" + "=" * 80)
    print("SYNC COMPLETE!")
    print("=" * 80)
    print(f"Splash Page Clocks records (valid): {len(clocking_df)}")
    print(f"Existing Timesheets records: {len(timesheets_df)}")
    print(f"Missing records found: {len(missing_df)}")
    print(f"New records created: {created_count}")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Exit with appropriate code
    if len(missing_df) > 0 and created_count == 0:
        print("\n⚠️  WARNING: Records were found but none were created. Check errors above.")
        exit(1)  # Non-zero exit code for CI/CD pipelines
    
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
    exit(1)  # Non-zero exit code for failure

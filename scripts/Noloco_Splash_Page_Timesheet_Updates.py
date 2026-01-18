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
    Download all records from Test Clocking Action table (Splash Page Clocks)
    
    Returns:
        DataFrame with columns: id, employee_id, employee_pin, clock_in, clock_out
    """
    print("Step 1: Downloading Splash Page Clocks records...")
    
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
        
        # Track how many records have missing data
        initial_count = len(df)
        
        # Remove records with missing required fields
        df = df.dropna(subset=["employee_pin", "clock_in", "clock_out"])
        
        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            print(f"  ⚠️  Filtered out {filtered_count} records with missing required fields")
        
        print(f"  ✓ Total valid records: {len(df)}")
        return df
        
    except Exception as e:
        raise Exception(f"Failed to download Splash Page Clocks: {str(e)}")


def download_timesheets():
    """
    Download all records from Timesheets table
    
    Returns:
        DataFrame with columns: id, employee_id, clock_in, clock_out
    """
    print("\nStep 2: Downloading Timesheets records...")
    
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
                    timesheetsCollection(first: 100, after: "{cursor}") {{
                        edges {{
                            node {{
                                id
                                employeeIdVal
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
                    "employee_id": node.get("employeeIdVal"),
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
        
        print(f"  ✓ Total records: {len(df)}")
        return df
        
    except Exception as e:
        raise Exception(f"Failed to download Timesheets: {str(e)}")


def get_employee_pin_mapping():
    """
    Fetch all employees and create a mapping from employeePin to Employee record ID
    
    Returns:
        Dictionary mapping employee_pin -> employee_record_id
    """
    print("\nStep 2.5: Fetching Employee records to map employee PINs...")
    
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
    Find records in Splash Page Clocks that don't exist in Timesheets
    
    We compare based on: employee_id + clock_in + clock_out
    If all three match, the record already exists
    
    Args:
        clocking_df: DataFrame from Splash Page Clocks
        timesheets_df: DataFrame from Timesheets
        
    Returns:
        DataFrame with missing records
    """
    print("\nStep 3: Finding missing records...")
    
    if len(clocking_df) == 0:
        print("  No records to check")
        return pd.DataFrame()
    
    try:
        # Create a unique key for each record (employee_id + clock_in + clock_out)
        clocking_df["match_key"] = (
            clocking_df["employee_id"].astype(str) + "_" +
            clocking_df["clock_in"].astype(str) + "_" +
            clocking_df["clock_out"].astype(str)
        )
        
        if len(timesheets_df) > 0:
            timesheets_df["match_key"] = (
                timesheets_df["employee_id"].astype(str) + "_" +
                timesheets_df["clock_in"].astype(str) + "_" +
                timesheets_df["clock_out"].astype(str)
            )
            
            # Find records in clocking that are NOT in timesheets
            missing_records = clocking_df[~clocking_df["match_key"].isin(timesheets_df["match_key"])].copy()
        else:
            # If timesheets is empty, all clocking records are missing
            missing_records = clocking_df.copy()
        
        # Remove the match_key column (we don't need it anymore)
        missing_records = missing_records.drop(columns=["match_key"])
        
        print(f"  ✓ Found {len(missing_records)} missing records")
        
        if len(missing_records) > 0:
            print("\n  Preview of missing records:")
            print(missing_records[["employee_id", "employee_pin", "clock_in", "clock_out"]].head(5).to_string(index=False))
        
        return missing_records
        
    except Exception as e:
        raise Exception(f"Failed to compare records: {str(e)}")


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
        print("\nStep 4: No records to upload")
        return 0
    
    print(f"\nStep 4: Uploading {len(records_df)} records to Timesheets...")
    
    created_count = 0
    failed_count = 0
    failed_reasons = {}  # Track failure reasons for summary
    
    # Upload each record one by one
    for index, row in records_df.iterrows():
        try:
            # Convert UTC times to Puerto Rico timezone
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
            print(f"  ✓ Created record {created_count}/{len(records_df)}: Employee {row['employee_id']} (PIN: {row['employee_pin']}) on {date_only}")
            
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
    print(f"\n  ✓ Successfully created: {created_count}")
    if failed_count > 0:
        print(f"  ✗ Failed: {failed_count}")
        print("\n  Failure breakdown:")
        for reason, count in failed_reasons.items():
            print(f"    - {reason}: {count}")
    
    return created_count


# Run the script
    
print("=" * 60)
print("Pet Esthetic Timesheet Sync")
print("=" * 60)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

try:
    # Step 1: Download Splash Page Clocks records
    clocking_df = download_test_clocking_actions()
    
    # Step 2: Download existing Timesheets
    timesheets_df = download_timesheets()
    
    # Step 2.5: Get employee PIN mapping (employee_pin -> employee_record_id)
    employee_pin_mapping = get_employee_pin_mapping()
    
    # Step 3: Find missing records
    missing_df = find_missing_records(clocking_df, timesheets_df)
    
    # Step 4: Upload missing records
    created_count = upload_to_timesheets(missing_df, employee_pin_mapping)
    
    # Summary
    print("\n" + "=" * 60)
    print("Sync Complete!")
    print("=" * 60)
    print(f"Splash Page Clocks records: {len(clocking_df)}")
    print(f"Existing Timesheets records: {len(timesheets_df)}")
    print(f"Missing records found: {len(missing_df)}")
    print(f"New records created: {created_count}")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Exit with appropriate code
    if len(missing_df) > 0 and created_count == 0:
        print("\n⚠️  WARNING: Records were found but none were created. Check errors above.")
        exit(1)  # Non-zero exit code for CI/CD pipelines
    
except Exception as e:
    print("\n" + "=" * 60)
    print("ERROR!")
    print("=" * 60)
    print(f"Sync failed: {str(e)}")
    print("\nTroubleshooting tips:")
    print("- Check your NOLOCO_API_TOKEN and NOLOCO_PROJECT_ID environment variables")
    print("- Verify your internet connection")
    print("- Check if Noloco API is accessible")
    print("- Review table structures haven't changed in Noloco")
    exit(1)  # Non-zero exit code for failure

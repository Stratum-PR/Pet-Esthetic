
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# ============================================================================
# CONFIGURATION - Update these with your credentials
# ============================================================================
# Load environment variables
load_dotenv()
API_TOKEN = os.getenv('NOLOCO_API_TOKEN')
PROJECT_ID = os.getenv('NOLOCO_PROJECT_ID')
API_URL = f"https://api.portals.noloco.io/data/{PROJECT_ID}"

# HTTP headers for API requests
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}
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


def run_graphql_query(query):
    """
    Send a GraphQL query to Noloco API and return the response
    
    Args:
        query: GraphQL query string
        
    Returns:
        Response data as dictionary
    """
    response = requests.post(
        API_URL,
        headers=HEADERS,
        json={"query": query},
        timeout=30
    )
    
    if response.status_code != 200:
        raise Exception(f"API error: {response.status_code} - {response.text}")
    
    result = response.json()
    
    if "errors" in result:
        raise Exception(f"GraphQL error: {result['errors']}")
    
    return result["data"]


def download_test_clocking_actions():
    """
    Download all records from Test Clocking Action table (Splash Page Clocks)
    
    Returns:
        DataFrame with columns: id, employee_id, clock_in, clock_out
    """
    print("Step 1: Downloading Test Clocking Action records...")
    
    all_records = []
    has_more_pages = True
    cursor = None
    page_number = 1
    
    # Keep fetching pages until we have all records
    while has_more_pages:
        # Build the query
        if cursor:
            # If we have a cursor, use it to get the next page
            query = f"""
            query {{
                testClockingActionCollection(first: 100, after: "{cursor}") {{
                    edges {{
                        node {{
                            id
                            employeeIdVal
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
            # First page - no cursor needed
            query = """
            query {
                testClockingActionCollection(first: 100) {
                    edges {
                        node {
                            id
                            employeeIdVal
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
    
    # Remove records with missing required fields
    df = df.dropna(subset=["employee_id", "clock_in", "clock_out"])
    
    print(f"  ✓ Total valid records: {len(df)}")
    return df


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
    
    # Keep fetching pages until we have all records
    while has_more_pages:
        # Build the query
        if cursor:
            # If we have a cursor, use it to get the next page
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
            # First page - no cursor needed
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


def get_user_id_mapping():
    """
    Fetch all users and create a mapping from employeeIdVal to User ID
    
    Returns:
        Dictionary mapping employee_id -> user_record_id
    """
    print("\nStep 2.5: Fetching User records to map employee IDs...")
    
    all_users = []
    has_more_pages = True
    cursor = None
    
    while has_more_pages:
        if cursor:
            query = f"""
            query {{
                userCollection(first: 100, after: "{cursor}") {{
                    edges {{
                        node {{
                            id
                            employeeIdVal
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
                userCollection(first: 100) {
                    edges {
                        node {
                            id
                            employeeIdVal
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
        collection = data.get("userCollection", {})
        edges = collection.get("edges", [])
        page_info = collection.get("pageInfo", {})
        
        for edge in edges:
            node = edge.get("node", {})
            if node.get("employeeIdVal"):  # Only add if they have an employee ID
                all_users.append({
                    "user_id": node.get("id"),
                    "employee_id": node.get("employeeIdVal")
                })
        
        has_more_pages = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")
    
    # Create mapping dictionary
    mapping = {user["employee_id"]: user["user_id"] for user in all_users}
    print(f"  ✓ Found {len(mapping)} users with employee IDs")
    
    return mapping


def find_missing_records(clocking_df, timesheets_df):
    """
    Find records in Test Clocking Action that don't exist in Timesheets
    
    We compare based on: employee_id + clock_in + clock_out
    If all three match, the record already exists
    
    Args:
        clocking_df: DataFrame from Test Clocking Action
        timesheets_df: DataFrame from Timesheets
        
    Returns:
        DataFrame with missing records
    """
    print("\nStep 3: Finding missing records...")
    
    if len(clocking_df) == 0:
        print("  No records to check")
        return pd.DataFrame()
    
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
        print(missing_records[["employee_id", "clock_in", "clock_out"]].head(5).to_string(index=False))
    
    return missing_records


def upload_to_timesheets(records_df, user_mapping):
    """
    Upload new records to Timesheets table
    
    Args:
        records_df: DataFrame with records to upload
        user_mapping: Dictionary mapping employee_id to user_id
        
    Returns:
        Number of successfully created records
    """
    if len(records_df) == 0:
        print("\nStep 4: No records to upload")
        return 0
    
    print(f"\nStep 4: Uploading {len(records_df)} records to Timesheets...")
    
    created_count = 0
    failed_count = 0
    
    # Upload each record one by one
    for index, row in records_df.iterrows():
        # Convert UTC times to Puerto Rico timezone
        clock_in_pr = convert_utc_to_pr(row['clock_in'])
        clock_out_pr = convert_utc_to_pr(row['clock_out'])
        
        # Extract just the date and create a datetime at midnight Puerto Rico time
        date_only = clock_in_pr.split('T')[0]  # Gets "2025-12-16"
        timesheet_date = f"{date_only}T00:00:00-04:00"  # Midnight PR time with timezone
        
        # Get the user ID for this employee
        user_id = user_mapping.get(row['employee_id'])
        
        if not user_id:
            print(f"  ⚠️  Skipping record {index + 1}: No user found for employee {row['employee_id']}")
            failed_count += 1
            continue
        
        # STEP 1: Create the timesheet record with the user link
        create_mutation = f"""
        mutation {{
            createTimesheets(
                employeeIdVal: "{row['employee_id']}",
                clockDatetime: "{clock_in_pr}",
                clockOutDatetime: "{clock_out_pr}",
                timesheetDate: "{timesheet_date}",
                usersId: "{user_id}"
            ) {{
                id
            }}
        }}
        """
        
        try:
            # Create the record
            result = run_graphql_query(create_mutation)
            timesheet_id = result.get("createTimesheets", {}).get("id")
            created_count += 1
            print(f"  ✓ Created record {created_count}/{len(records_df)}: Employee {row['employee_id']} on {date_only} (linked to user)")
            
        except Exception as e:
            failed_count += 1
            print(f"  ✗ Failed record {index + 1}: {str(e)}")
    
    print(f"\n  ✓ Successfully created: {created_count}")
    if failed_count > 0:
        print(f"  ✗ Failed: {failed_count}")
    
    return created_count

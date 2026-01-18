import requests
import os
from dotenv import load_dotenv

# Load environment variables
API_TOKEN = os.getenv('NOLOCO_API_TOKEN')
PROJECT_ID = os.getenv('NOLOCO_PROJECT_ID')
API_URL = f"https://api.portals.noloco.io/data/{PROJECT_ID}"

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# GraphQL introspection query to get mutation arguments
introspection_query = """
{
  __type(name: "Mutation") {
    name
    fields {
      name
      args {
        name
        type {
          name
          kind
          ofType {
            name
            kind
          }
        }
      }
    }
  }
}
"""

print("Fetching Mutation schema...")
print("=" * 80)

response = requests.post(
    API_URL,
    headers=HEADERS,
    json={"query": introspection_query},
    timeout=30
)

if response.status_code != 200:
    print(f"Error: {response.status_code}")
    print(response.text)
    exit(1)

result = response.json()

if "errors" in result:
    print(f"GraphQL Error: {result['errors']}")
    exit(1)

# Find createTimesheets mutation
type_info = result["data"]["__type"]
create_timesheets = None

for field in type_info['fields']:
    if field['name'] == 'createTimesheets':
        create_timesheets = field
        break

if not create_timesheets:
    print("createTimesheets mutation not found!")
    exit(1)

print("createTimesheets mutation arguments:")
print("=" * 80)

for arg in sorted(create_timesheets['args'], key=lambda x: x['name']):
    arg_name = arg['name']
    arg_type = arg['type']
    
    # Get the type name
    if arg_type['name']:
        type_name = arg_type['name']
    elif arg_type['ofType']:
        type_name = arg_type['ofType']['name']
    else:
        type_name = "Unknown"
    
    print(f"{arg_name:<40} -> {type_name}")

print("\n" + "=" * 80)
print("Looking for employee-related arguments:")
print("=" * 80)

for arg in create_timesheets['args']:
    arg_name = arg['name'].lower()
    if 'employee' in arg_name or 'user' in arg_name or 'id' in arg_name:
        print(f"✓ {arg['name']}")

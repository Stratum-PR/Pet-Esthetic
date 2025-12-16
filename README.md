<div align="center">
  <img src="https://rest.techbehemoths.com/storage/images/users/main/company-avatar-6913307ce5829-x2.png" alt="Stratum PR" width="200" style="margin-right: 20px"/>
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
  <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRD_nbaHVVaKtOPRBACWcIyulQUeigNTlgiwQ&sE" alt="Pet Esthetic" width="200" style="margin: 20px"/>
</div>

# Pet-Esthetic

Automated timesheet synchronization system for Pet Esthetic employee time tracking.

## Overview

This system automatically syncs employee clock-in/clock-out records from a splash page interface to a centralized timesheet database. Built to integrate with Noloco's low-code platform, it ensures accurate time tracking and proper assignment of timesheet entries to employee accounts.

The automation runs twice daily (11 AM and 11 PM Puerto Rico time) via GitHub Actions, eliminating manual data entry and reducing timesheet errors.

## Repository Structure

```
pet-esthetic-timesheet-sync/
├── .github/
│   └── workflows/
│       └── Daily_Splash_Page_Timesheet_Sync.yml            # GitHub Actions workflow
├── scripts/
│       └── Noloco_Splash_Page_Timesheet_Updates.py         # Main synchronization script
└── README.md                                               # This file
```

## Scripts

### `Noloco_Splash_Page_Timesheet_Updates.py`
Main synchronization script that performs the complete sync workflow:
- Downloads all clock-in/out records from Test Clocking Action table
- Retrieves existing timesheet entries
- Identifies missing records by comparing employee ID, clock-in, and clock-out timestamps
- Creates new timesheet entries with proper timezone conversion (UTC to Puerto Rico AST)
- Links timesheet entries to employee user accounts
- Generates detailed execution logs

See [SCRIPT_README.md](scripts/Timesheets_Update_README.md) for detailed documentation.

### `.github/workflows/Daily_Splash_Page_Timesheet_Sync.yml`
GitHub Actions workflow definition that orchestrates automated execution:
- Scheduled runs at 11 AM and 11 PM Puerto Rico time
- Configures Python environment and installs dependencies
- Executes synchronization script with secure credential management
- Uploads execution logs as artifacts for troubleshooting

## Technology Stack

**Language:** Python 3.11

**Core Dependencies:**
- `requests` - HTTP client for GraphQL API communication
- `pandas` - Data manipulation and analysis
- `zoneinfo` - Timezone handling for UTC to Puerto Rico conversion

**Platform:** 
- Noloco GraphQL API
- GitHub Actions for automation

## Features

- **Automatic Synchronization:** Runs twice daily without manual intervention
- **Timezone Handling:** Converts UTC timestamps to Puerto Rico Atlantic Standard Time (AST/UTC-4)
- **Duplicate Prevention:** Identifies and skips records that already exist in the timesheet system
- **User Linking:** Automatically associates timesheet entries with employee user accounts
- **Pagination Support:** Handles large datasets by fetching records in batches
- **Error Handling:** Comprehensive error catching with detailed logging
- **Secure Credential Management:** Uses GitHub Secrets for API token storage

## Data Flow

1. **Source:** Test Clocking Action table (employee splash page clock-ins)
2. **Processing:** Python script compares and identifies missing records
3. **Destination:** Timesheets table (centralized employee timesheet system)
4. **Linking:** Employee_Record field connects timesheets to user accounts

## Field Mapping

| Source Field (Test Clocking Action) | Destination Field (Timesheets) | Description |
|-------------------------------------|--------------------------------|-------------|
| `employeeIdVal` | `employeeIdVal` | Employee identifier |
| `clockIn` | `clockDatetime` | Clock-in timestamp (converted to PR time) |
| `clockOut` | `clockOutDatetime` | Clock-out timestamp (converted to PR time) |
| `clockIn` (date only) | `timesheetDate` | Date of timesheet entry |
| Employee user ID lookup | `usersId` | Links to employee user account |


## Security Considerations

- Repository is maintained as **private** to protect business logic and API access patterns
- API credentials stored as GitHub Secrets, never committed to version control
- Script supports environment variables for production and hardcoded fallback for local testing
- Rate limiting and error handling prevent API abuse

## Monitoring

Execution logs are automatically generated and stored as GitHub Actions artifacts for 7 days. Workflow failures trigger email notifications to repository administrators.

View execution history: Repository → Actions tab → Daily Timesheet Sync workflow

## Maintenance

**Update Synchronization Schedule:**
Edit cron expressions in `.github/workflows/Daily_Splash_Page_Timesheet_Sync.yml`

**Modify Field Mappings:**
Update field names in `Noloco_Splash_Page_Timesheet_Updates.py` functions:
- `download_test_clocking_actions()`
- `download_timesheets()`
- `upload_to_timesheets()`

**Rotate API Credentials:**
Update `NOLOCO_API_TOKEN` secret in repository settings

## Requirements

- Python 3.11 or higher
- Active Noloco account with API access
- GitHub account (for automated deployment)
- Valid API token with read/write permissions for Test Clocking Action and Timesheets collections

## License

Proprietary - Internal use only for Pet Esthetic operations.

## Support

For issues or questions regarding this automation system, contact the development team or review the execution logs in the GitHub Actions tab.

---

**Last Updated:** December 2024  
**Status:** Production  
**Automation:** Active (11 AM & 11 PM AST)

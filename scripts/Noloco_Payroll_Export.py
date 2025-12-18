import pandas as pd
import requests
from datetime import datetime, date
from typing import Optional, List, Tuple, Dict
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import base64
import os

pd.set_option('display.max_columns', None)

# ============================================================================
# CONFIGURATION
# ============================================================================
API_KEY = os.getenv('NOLOCO_API_TOKEN')
APP_SLUG = os.getenv('NOLOCO_PROJECT_ID')

# ============================================================================
# DATA FETCHING FUNCTIONS  
# ============================================================================

def get_table_fields(app_name: str, api_key: str, table_name: str) -> Tuple[List[str], Dict[str, str]]:
    """Get scalar and relationship fields from a Noloco table."""
    base_url = f"https://api.portals.noloco.io/data/{app_name}"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    query = f'''query {{ __type(name: "{table_name}") {{ fields {{ name type {{ name kind ofType {{ name kind }} }} }} }} }}'''
    
    response = requests.post(base_url, headers=headers, json={"query": query})
    response.raise_for_status()
    result = response.json()
    
    if 'errors' in result:
        raise Exception(f"GraphQL Error: {result['errors'][0].get('message')}")
    
    fields = result['data']['__type']['fields']
    scalar_fields, relationship_fields = [], {}
    scalar_types = {'String', 'Int', 'Float', 'Boolean', 'ID', 'DateTime', 'Date'}
    
    for field in fields:
        field_name = field['name']
        actual_type = field['type']
        while actual_type.get('kind') in ['LIST', 'NON_NULL']:
            actual_type = actual_type.get('ofType', {})
        
        type_name, type_kind = actual_type.get('name'), actual_type.get('kind')
        if type_name and 'Connection' in type_name:
            continue
        
        if type_kind in ['SCALAR', 'ENUM'] or type_name in scalar_types:
            scalar_fields.append(field_name)
        elif type_kind == 'OBJECT':
            relationship_fields[field_name] = type_name
        else:
            scalar_fields.append(field_name)
    
    return scalar_fields, relationship_fields


def download_noloco_data(app_name: str, api_key: str, table_name: str, fields: List[str], relationship_fields: Optional[dict] = None) -> pd.DataFrame:
    """Download data from Noloco with pagination."""
    base_url = f"https://api.portals.noloco.io/data/{app_name}"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    query_name = table_name.lower() + 'Collection'
    
    fields_str = '\n                '.join(fields)
    relationship_str = ''
    
    if relationship_fields:
        rel_parts = []
        for rel_field, rel_subfields in relationship_fields.items():
            subfields_str = '\n                  '.join(rel_subfields)
            rel_parts.append(f"{rel_field} {{\n                  {subfields_str}\n                }}")
        relationship_str = '\n                ' + '\n                '.join(rel_parts)
    
    all_records = []
    has_next_page = True
    after_cursor = None
    
    while has_next_page:
        after_str = f', after: "{after_cursor}"' if after_cursor else ''
        query = f'''
        query {{
          {query_name}(first: 100{after_str}) {{
            edges {{
              node {{
                {fields_str}{relationship_str}
              }}
            }}
            pageInfo {{
              hasNextPage
              endCursor
            }}
          }}
        }}
        '''
        
        response = requests.post(base_url, headers=headers, json={"query": query})
        response.raise_for_status()
        result = response.json()
        
        if 'errors' in result:
            raise Exception(f"GraphQL Error: {result['errors']}")
        
        data = result['data'][query_name]
        records = [edge['node'] for edge in data['edges']]
        
        if relationship_fields:
            flattened = []
            for record in records:
                flat = record.copy()
                for rel_field in relationship_fields.keys():
                    if rel_field in flat and isinstance(flat[rel_field], dict):
                        nested = flat.pop(rel_field)
                        for key, value in nested.items():
                            flat[f"{rel_field}_{key}"] = value
                flattened.append(flat)
            records = flattened
        
        all_records.extend(records)
        has_next_page = data['pageInfo']['hasNextPage']
        after_cursor = data['pageInfo'].get('endCursor')
        print(f"Fetched {len(records)} records (Total: {len(all_records)})")
    
    return pd.DataFrame(all_records)


def download_all_fields(app_name: str, api_key: str, table_name: str) -> pd.DataFrame:
    """Auto-discover and download all fields from a table."""
    relationship_subfields = ["id", "uuid", "fullName", "payRate"]
    
    scalar_fields, relationship_fields = get_table_fields(app_name, api_key, table_name)
    relationship_config = {}
    for field_name, related_type in relationship_fields.items():
        try:
            related_scalar_fields, _ = get_table_fields(app_name, api_key, related_type)
            available_subfields = [f for f in relationship_subfields if f in related_scalar_fields]
            if not available_subfields:
                available_subfields = ["id"] if "id" in related_scalar_fields else related_scalar_fields[:1]
            relationship_config[field_name] = available_subfields
        except:
            continue
    
    return download_noloco_data(app_name, api_key, table_name, scalar_fields, relationship_config)


# ============================================================================
# SHEET CREATION FUNCTIONS
# ============================================================================

def create_main_timesheet_sheet(wb, df, styles, company_name, period_text):
    """Sheet 1: Main detailed timesheet"""
    ws = wb.create_sheet("Time Entries")
    current_row = 1
    
    ws[f'A{current_row}'] = f"{company_name} - PAYROLL TIMESHEET" if company_name else "PAYROLL TIMESHEET"
    ws[f'A{current_row}'].font = styles['title_font']
    current_row += 1
    
    ws[f'A{current_row}'] = f"Pay Period: {period_text}"
    ws[f'A{current_row}'].font = Font(bold=True, size=11)
    current_row += 1
    
    ws[f'A{current_row}'] = f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}"
    ws[f'A{current_row}'].font = Font(italic=True, size=10)
    current_row += 2
    
    # Headers
    headers = ['Employee ID', 'Employee Name', 'Date', 'Clock In', 'Clock Out', 'Hours', 'Status', 'Period Start', 'Period End']
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=current_row, column=col_num)
        cell.value = header
        cell.font = styles['header_font']
        cell.fill = styles['header_fill']
        cell.alignment = Alignment(horizontal='center')
        cell.border = styles['border']
    
    current_row += 1
    start_data_row = current_row
    
    # Data rows
    for idx, row in df.iterrows():
        col = 1
        ws.cell(row=current_row, column=col).value = row.get('employeeIdVal', '')
        ws.cell(row=current_row, column=col).border = styles['border']
        col += 1
        
        ws.cell(row=current_row, column=col).value = row.get('users_fullName', '')
        ws.cell(row=current_row, column=col).border = styles['border']
        col += 1
        
        if 'timesheetDate' in row and pd.notna(row['timesheetDate']):
            ws.cell(row=current_row, column=col).value = pd.to_datetime(row['timesheetDate']).strftime('%m/%d/%Y')
        ws.cell(row=current_row, column=col).border = styles['border']
        col += 1
        
        if 'clockDatetime' in row and pd.notna(row['clockDatetime']):
            ws.cell(row=current_row, column=col).value = pd.to_datetime(row['clockDatetime']).strftime('%I:%M %p')
        ws.cell(row=current_row, column=col).border = styles['border']
        col += 1
        
        if 'clockOutDatetime' in row and pd.notna(row['clockOutDatetime']):
            ws.cell(row=current_row, column=col).value = pd.to_datetime(row['clockOutDatetime']).strftime('%I:%M %p')
        ws.cell(row=current_row, column=col).border = styles['border']
        col += 1
        
        cell = ws.cell(row=current_row, column=col)
        cell.value = row.get('shiftHoursWorked', 0)
        cell.number_format = '0.00' if cell.value > 0 else '0'
        cell.border = styles['border']
        col += 1
        
        cell = ws.cell(row=current_row, column=col)
        approved = row.get('approved', False)
        cell.value = 'Approved' if approved else 'Pending'
        cell.fill = styles['approved_fill'] if approved else styles['pending_fill']
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='center')
        col += 1
        
        if 'periodStartDate' in row and pd.notna(row['periodStartDate']):
            ws.cell(row=current_row, column=col).value = pd.to_datetime(row['periodStartDate']).strftime('%m/%d/%Y')
        ws.cell(row=current_row, column=col).border = styles['border']
        col += 1
        
        if 'periodEndDate' in row and pd.notna(row['periodEndDate']):
            ws.cell(row=current_row, column=col).value = pd.to_datetime(row['periodEndDate']).strftime('%m/%d/%Y')
        ws.cell(row=current_row, column=col).border = styles['border']
        
        current_row += 1
    
    # Total
    current_row += 1
    ws[f'A{current_row}'] = "TOTAL"
    ws[f'A{current_row}'].font = Font(bold=True)
    cell = ws.cell(row=current_row, column=6)
    cell.value = f"=SUM(F{start_data_row}:F{current_row-2})"
    cell.font = Font(bold=True)
    cell.number_format = '0.00'
    cell.border = Border(top=Side(style='double'), bottom=Side(style='double'))
    
    # Column widths
    widths = [12, 25, 12, 12, 12, 10, 12, 13, 13]
    for i, width in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = width


def create_employee_summary_sheet(wb, df, styles, company_name, period_text):
    """Sheet 2: Grouped by employee"""
    ws = wb.create_sheet("Employee Summary")
    current_row = 1
    
    ws[f'A{current_row}'] = f"{company_name} - BY EMPLOYEE SUMMARY" if company_name else "BY EMPLOYEE SUMMARY"
    ws[f'A{current_row}'].font = styles['title_font']
    current_row += 1
    
    ws[f'A{current_row}'] = f"Pay Period: {period_text}"
    ws[f'A{current_row}'].font = Font(bold=True, size=11)
    current_row += 3
    
    employee_col = 'employeeIdVal' if 'employeeIdVal' in df.columns else 'id'
    name_col = 'users_fullName' if 'users_fullName' in df.columns else 'Unknown'
    
    for employee_id in df[employee_col].unique():
        employee_df = df[df[employee_col] == employee_id]
        employee_name = employee_df[name_col].iloc[0] if name_col in df.columns else 'Unknown'
        
        ws[f'A{current_row}'] = f"Employee: {employee_name} (ID: {employee_id})"
        ws[f'A{current_row}'].font = Font(bold=True, size=11)
        current_row += 1
        
        headers = ['Date', 'Clock In', 'Clock Out', 'Hours', 'Status']
        for col_num, header in enumerate(headers, 1):
            cell = ws.cell(row=current_row, column=col_num)
            cell.value = header
            cell.font = styles['header_font']
            cell.fill = styles['header_fill']
            cell.alignment = Alignment(horizontal='center')
            cell.border = styles['border']
        
        current_row += 1
        start_row = current_row
        
        for idx, row in employee_df.iterrows():
            col = 1
            if 'timesheetDate' in row and pd.notna(row['timesheetDate']):
                ws.cell(row=current_row, column=col).value = pd.to_datetime(row['timesheetDate']).strftime('%m/%d/%Y')
            ws.cell(row=current_row, column=col).border = styles['border']
            col += 1
            
            if 'clockDatetime' in row and pd.notna(row['clockDatetime']):
                ws.cell(row=current_row, column=col).value = pd.to_datetime(row['clockDatetime']).strftime('%I:%M %p')
            ws.cell(row=current_row, column=col).border = styles['border']
            col += 1
            
            if 'clockOutDatetime' in row and pd.notna(row['clockOutDatetime']):
                ws.cell(row=current_row, column=col).value = pd.to_datetime(row['clockOutDatetime']).strftime('%I:%M %p')
            ws.cell(row=current_row, column=col).border = styles['border']
            col += 1
            
            cell = ws.cell(row=current_row, column=col)
            cell.value = row.get('shiftHoursWorked', 0)
            cell.number_format = '0.00' if cell.value > 0 else '0'
            cell.border = styles['border']
            col += 1
            
            cell = ws.cell(row=current_row, column=col)
            approved = row.get('approved', False)
            cell.value = 'Approved' if approved else 'Pending'
            cell.fill = styles['approved_fill'] if approved else styles['pending_fill']
            cell.border = styles['border']
            cell.alignment = Alignment(horizontal='center')
            
            current_row += 1
        
        ws[f'A{current_row}'] = f"Subtotal - {employee_name}"
        ws[f'A{current_row}'].font = Font(bold=True)
        cell = ws.cell(row=current_row, column=4)
        cell.value = f"=SUM(D{start_row}:D{current_row-1})"
        cell.font = Font(bold=True)
        cell.number_format = '0.00'
        cell.border = Border(top=Side(style='thin'), bottom=Side(style='double'))
        current_row += 3


def create_pay_calculations_sheet(wb, df, styles, company_name, period_text, hourly_rate=None):
    """Sheet 3: Pay calculations with formulas"""
    ws = wb.create_sheet("Payroll")
    current_row = 1
    
    ws[f'A{current_row}'] = f"{company_name} - PAY CALCULATIONS" if company_name else "PAY CALCULATIONS"
    ws[f'A{current_row}'].font = styles['title_font']
    current_row += 1
    
    ws[f'A{current_row}'] = f"Pay Period: {period_text}"
    ws[f'A{current_row}'].font = Font(bold=True, size=11)
    current_row += 2
    
    ws[f'A{current_row}'] = "Note: Pay rates are editable. Gross Pay is calculated as Hours × Rate."
    ws[f'A{current_row}'].font = Font(italic=True, size=10)
    current_row += 2
    
    headers = ['Employee ID', 'Employee Name', 'Total Hours', 'Hourly Rate', 'Gross Pay']
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=current_row, column=col_num)
        cell.value = header
        cell.font = styles['header_font']
        cell.fill = styles['header_fill']
        cell.alignment = Alignment(horizontal='center', wrap_text=True)
        cell.border = styles['border']
    
    current_row += 1
    start_data_row = current_row
    
    employee_col = 'employeeIdVal' if 'employeeIdVal' in df.columns else 'id'
    name_col = 'users_fullName' if 'users_fullName' in df.columns else 'Unknown'
    pay_rate_col = 'users_payRate' if 'users_payRate' in df.columns else None
    
    for employee_id in df[employee_col].unique():
        employee_df = df[df[employee_col] == employee_id]
        employee_name = employee_df[name_col].iloc[0] if name_col in df.columns else 'Unknown'
        total_hours = employee_df['shiftHoursWorked'].sum() if 'shiftHoursWorked' in df.columns else 0
        
        if pay_rate_col and pay_rate_col in df.columns and pd.notna(employee_df[pay_rate_col].iloc[0]):
            employee_rate = float(employee_df[pay_rate_col].iloc[0])
        elif hourly_rate:
            employee_rate = hourly_rate
        else:
            employee_rate = 15.00
        
        col = 1
        ws.cell(row=current_row, column=col).value = employee_id
        ws.cell(row=current_row, column=col).border = styles['border']
        col += 1
        
        ws.cell(row=current_row, column=col).value = employee_name
        ws.cell(row=current_row, column=col).border = styles['border']
        col += 1
        
        cell = ws.cell(row=current_row, column=col)
        cell.value = total_hours
        cell.number_format = '0.00' if total_hours > 0 else '0'
        cell.border = styles['border']
        hours_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        cell = ws.cell(row=current_row, column=col)
        cell.value = employee_rate
        cell.number_format = '$#,##0.00'
        cell.border = styles['border']
        rate_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # Gross Pay (simple: hours * rate)
        cell = ws.cell(row=current_row, column=col)
        cell.value = f'={hours_cell}*{rate_cell}'
        cell.number_format = '$#,##0.00'
        cell.border = styles['border']
        cell.font = Font(bold=True)
        
        current_row += 1
    
    # Totals
    current_row += 1
    ws[f'A{current_row}'] = "TOTALS"
    ws[f'A{current_row}'].font = Font(bold=True)
    
    # Total Hours (column 3)
    cell = ws.cell(row=current_row, column=3)
    cell.value = f'=SUM(C{start_data_row}:C{current_row-2})'
    cell.font = Font(bold=True)
    cell.number_format = '0.00'
    cell.border = Border(top=Side(style='double'), bottom=Side(style='double'))
    
    # Total Gross Pay (column 5)
    cell = ws.cell(row=current_row, column=5)
    cell.value = f'=SUM(E{start_data_row}:E{current_row-2})'
    cell.font = Font(bold=True)
    cell.number_format = '$#,##0.00'
    cell.border = Border(top=Side(style='double'), bottom=Side(style='double'))


# ============================================================================
# MAIN REPORT FUNCTION
# ============================================================================

def create_comprehensive_payroll_report(
    df: pd.DataFrame,
    output_filename: str = "Pet_Esthetic_Payroll_Report.xlsx",
    company_name: str = "Pet Esthetic",
    pay_period_label: str = None,
    hourly_rate: float = None
):
    """Create comprehensive payroll report with all 3 sheets."""
    df = df.copy()
    
    # Parse datetime columns and convert from UTC to Puerto Rico time (AST, UTC-4)
    for col in ['clockDatetime', 'clockOutDatetime', 'approvedDate', 'periodStartDate', 'periodEndDate', 'timesheetDate']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
            # Convert to Puerto Rico time (AST = UTC-4)
            df[col] = df[col].dt.tz_convert('America/Puerto_Rico')
    
    # Determine pay period from the data
    if pay_period_label:
        period_text = pay_period_label
    elif 'periodStartDate' in df.columns and 'periodEndDate' in df.columns:
        # Get the period dates from the data (already converted to local time)
        start = df['periodStartDate'].min()
        end = df['periodEndDate'].max()
        period_text = f"{start.strftime('%B %d')} - {end.strftime('%B %d, %Y')}"
    else:
        period_text = "N/A"
    
    # Create workbook
    wb = Workbook()
    wb.remove(wb.active)
    
    # Define styles
    styles = {
        'header_fill': PatternFill(start_color="EB7979", end_color="EB7979", fill_type="solid"),
        'header_font': Font(bold=True, color="FFFFFF", size=11),
        'title_font': Font(bold=True, size=14),
        'section_font': Font(bold=True, size=12),
        'approved_fill': PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid"),
        'pending_fill': PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid"),
        'border': Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
    }
    
    # Create all 3 sheets
    create_main_timesheet_sheet(wb, df, styles, company_name, period_text)
    create_employee_summary_sheet(wb, df, styles, company_name, period_text)
    create_pay_calculations_sheet(wb, df, styles, company_name, period_text, hourly_rate)
    
    # Save
    wb.save(output_filename)
    
    print(f"✓ Comprehensive payroll report created: {output_filename}")
    print(f"  - {len(df)} total entries")
    print(f"  - {df['employeeIdVal'].nunique() if 'employeeIdVal' in df.columns else 'N/A'} employees")
    print(f"  - 3 sheets: Time Entries, Employee Summary, Payroll")
    
    return output_filename


def upload_file_to_noloco(app_name: str, api_key: str, file_path: str, pay_period_text: str = None) -> str:
    """
    Upload a file to Noloco Documents table using multipart/form-data
    
    Args:
        app_name: Noloco app slug (e.g., 'pet-esthetic')
        api_key: API authentication key
        file_path: Local path to the file to upload
        pay_period_text: Pay period text for notes (e.g., "December 07 - December 18, 2025")
        
    Returns:
        Document ID if successful
    """
    import json
    
    # Get filename
    filename = os.path.basename(file_path)
    
    # Create document name in format: yyyy-mm-dd_Payroll_Report
    document_name = f"{date.today().strftime('%Y-%m-%d')}_Payroll_Report"
    
    # Determine MIME type
    if filename.endswith('.xlsx'):
        mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif filename.endswith('.xls'):
        mime_type = 'application/vnd.ms-excel'
    elif filename.endswith('.pdf'):
        mime_type = 'application/pdf'
    else:
        mime_type = 'application/octet-stream'
    
    # Create notes with pay period information
    notes = f"Pay Period: {pay_period_text}" if pay_period_text else "Payroll Report"
    
    # Escape special characters for JSON
    notes_escaped = notes.replace('\\', '\\\\').replace('"', '\\"')
    document_name_escaped = document_name.replace('\\', '\\\\').replace('"', '\\"')
    
    base_url = f"https://api.portals.noloco.io/data/{app_name}"
    
    # GraphQL multipart request specification
    # https://github.com/jaydenseric/graphql-multipart-request-spec
    
    # The GraphQL operation
    operations = {
        "query": """
            mutation($documentName: String, $documentType: String, $notes: String, $document: [Upload!]) {
                createDocuments(
                    documentName: $documentName,
                    documentType: $documentType,
                    notes: $notes,
                    document: $document
                ) {
                    id
                    documentName
                    documentType
                }
            }
        """,
        "variables": {
            "documentName": document_name,
            "documentType": "Biweekly Payroll",
            "notes": notes,
            "document": [None]  # Will be mapped to file
        }
    }
    
    # Map showing which variable paths map to which files
    map_data = {
        "0": ["variables.document.0"]
    }
    
    # Prepare multipart form data
    files = {
        'operations': (None, json.dumps(operations), 'application/json'),
        'map': (None, json.dumps(map_data), 'application/json'),
        '0': (filename, open(file_path, 'rb'), mime_type)
    }
    
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    
    # Note: Don't set Content-Type header - requests will set it with boundary
    response = requests.post(base_url, headers=headers, files=files)
    
    # Close the file
    files['0'][1].close()
    
    response.raise_for_status()
    result = response.json()
    
    if 'errors' in result:
        raise Exception(f"GraphQL Error: {result['errors']}")
    
    document_id = result['data']['createDocuments']['id']
    print(f"✓ File uploaded to Noloco Documents table")
    print(f"  - Document ID: {document_id}")
    print(f"  - Document Name: {document_name}")
    print(f"  - Document Type: Biweekly Payroll")
    print(f"  - Notes: {notes}")
    
    return document_id


# ============================================================================
# MAIN EXECUTION
# ============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("NOLOCO PAYROLL EXPORT - 3 ESSENTIAL SHEETS")
    print("=" * 60)
    
    print("\n[1/3] Downloading timesheet data from Noloco...")
    df = download_all_fields(APP_SLUG, API_KEY, "Timesheets")
    print(f"\n✓ Dataset shape: {df.shape}")
    print(f"✓ Total entries: {len(df)}")
    
    print("\n[2/3] Generating comprehensive payroll report with all sheets...")
    output_filename = date.today().strftime('%Y-%m-%d') + " Pet Esthetic Payroll Report.xlsx"
    create_comprehensive_payroll_report(
        df=df,
        output_filename=output_filename,
        company_name="Pet Esthetic"
    )
    
    print("\n[3/3] Uploading report to Noloco Documents...")
    try:
        # Extract pay period from data for notes
        if 'periodStartDate' in df.columns and 'periodEndDate' in df.columns:
            # Dates are already timezone-aware datetime objects
            start_date = pd.to_datetime(df['periodStartDate'].min()).strftime('%Y-%m-%d')
            end_date = pd.to_datetime(df['periodEndDate'].max()).strftime('%Y-%m-%d')
            pay_period_text = f"{start_date} - {end_date}"
        else:
            pay_period_text = None
        
        document_id = upload_file_to_noloco(
            app_name=APP_SLUG,
            api_key=API_KEY,
            file_path=output_filename,
            pay_period_text=pay_period_text
        )
    except Exception as e:
        print(f"⚠️  Could not upload to Noloco: {str(e)}")
        print("   File is still available locally")
    
    print("\n" + "=" * 60)
    print("✅ ALL DONE! Report has 3 essential sheets")
    print("=" * 60)
    print(f"\nLocal file: {output_filename}")
    print(f"Noloco Document ID: {document_id if 'document_id' in locals() else 'Upload failed'}")

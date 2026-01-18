# UPDATED create_pay_calculations_sheet function with manual commission entry
# Replace this function in your existing payroll script

from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

def create_pay_calculations_sheet(wb, df, styles, company_name, period_text, hourly_rate=None, min_wage_rate=10.50):
    """
    Sheet 3: Pay calculations with MANUAL commission and hourly rate entry
    
    Features:
    - Gray highlighted column for MANUAL commission entry
    - Gray highlighted column for MANUAL hourly rate entry (editable for raises/changes)
    - Empty hourly rates are pre-filled with minimum wage ($10.50 in Puerto Rico)
    - Automatic minimum wage guarantee calculation
    - Formula automatically pays MAX(commission, minimum_wage) for commission employees
    - Standard hourly pay for hourly employees
    - Payment type indicator showing how each employee was paid
    
    Args:
        wb: Workbook object
        df: DataFrame with timesheet data
        styles: Dictionary of cell styles
        company_name: Company name for header
        period_text: Pay period text
        hourly_rate: Default hourly rate if not in user data (pre-fills the editable field)
        min_wage_rate: Minimum wage rate (default $10.50 for Puerto Rico as of 2024)
    """
    ws = wb.create_sheet("Payroll")
    current_row = 1
    
    # Title
    ws[f'A{current_row}'] = f"{company_name} - PAY CALCULATIONS" if company_name else "PAY CALCULATIONS"
    ws[f'A{current_row}'].font = styles['title_font']
    current_row += 1
    
    # Period
    ws[f'A{current_row}'] = f"Pay Period: {period_text}"
    ws[f'A{current_row}'].font = Font(bold=True, size=11)
    current_row += 2
    
    # Min Wage Rate Display (so users know what it is)
    ws[f'A{current_row}'] = f"Current Minimum Wage Rate: ${min_wage_rate:.2f}/hour (Puerto Rico)"
    ws[f'A{current_row}'].font = Font(bold=True, size=10, color="0000FF")
    current_row += 2
    
    # Instructions (highlighted in red for visibility)
    ws[f'A{current_row}'] = "INSTRUCTIONS:"
    ws[f'A{current_row}'].font = Font(bold=True, size=11, color="FF0000")
    current_row += 1
    
    ws[f'A{current_row}'] = "1. GRAY columns are EDITABLE - Update hourly rates if employee got a raise"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 1
    
    ws[f'A{current_row}'] = "2. For COMMISSION employees: Enter total commission $ in the gray 'Commission $' column"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 1
    
    ws[f'A{current_row}'] = f"3. System compares commission vs minimum wage guarantee (${min_wage_rate}/hr × hours worked)"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 1
    
    ws[f'A{current_row}'] = "4. Commission employees receive whichever is HIGHER (commission or minimum wage)"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 1
    
    ws[f'A{current_row}'] = "5. HOURLY employees are paid: Hours × Hourly Rate (commission column is ignored)"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 1
    
    ws[f'A{current_row}'] = "6. All calculations update automatically when you change rates or commission amounts"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 2
    
    # Headers
    headers = [
        'Employee ID', 
        'Employee Name', 
        'Employee Type',
        'Total Hours', 
        'Hourly Rate',
        'Commission $',
        'Min Wage\nGuarantee',
        'Gross Pay',
        'Payment Type'
    ]
    
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=current_row, column=col_num)
        cell.value = header
        cell.font = styles['header_font']
        cell.fill = styles['header_fill']
        cell.alignment = Alignment(horizontal='center', wrap_text=True, vertical='center')
        cell.border = styles['border']
    
    current_row += 1
    start_data_row = current_row
    
    # Column references
    employee_col = 'employeeIdVal' if 'employeeIdVal' in df.columns else 'id'
    name_col = 'users_fullName' if 'users_fullName' in df.columns else 'Unknown'
    pay_rate_col = 'users_payRate' if 'users_payRate' in df.columns else None
    employee_type_col = 'users_employeeType' if 'users_employeeType' in df.columns else None
    
    # Process each employee
    for employee_id in df[employee_col].unique():
        employee_df = df[df[employee_col] == employee_id]
        employee_name = employee_df[name_col].iloc[0] if name_col in df.columns else 'Unknown'
        total_hours = employee_df['shiftHoursWorked'].sum() if 'shiftHoursWorked' in df.columns else 0
        
        # Get employee type (default to Hourly if not specified)
        if employee_type_col and employee_type_col in df.columns and pd.notna(employee_df[employee_type_col].iloc[0]):
            employee_type = str(employee_df[employee_type_col].iloc[0])
        else:
            employee_type = 'Hourly'  # Default assumption
        
        # Get hourly rate (this will PRE-FILL the editable field)
        # IF EMPTY OR MISSING, USE MINIMUM WAGE RATE
        if pay_rate_col and pay_rate_col in df.columns and pd.notna(employee_df[pay_rate_col].iloc[0]):
            rate_value = employee_df[pay_rate_col].iloc[0]
            # Check if rate is a valid number and greater than 0
            if rate_value and float(rate_value) > 0:
                employee_rate = float(rate_value)
            else:
                employee_rate = min_wage_rate  # Use minimum wage if empty/zero
        elif hourly_rate and hourly_rate > 0:
            employee_rate = hourly_rate
        else:
            employee_rate = min_wage_rate  # Default to minimum wage
        
        col = 1
        
        # Employee ID
        ws.cell(row=current_row, column=col).value = employee_id
        ws.cell(row=current_row, column=col).border = styles['border']
        ws.cell(row=current_row, column=col).alignment = Alignment(horizontal='center')
        col += 1
        
        # Employee Name
        ws.cell(row=current_row, column=col).value = employee_name
        ws.cell(row=current_row, column=col).border = styles['border']
        col += 1
        
        # Employee Type
        cell = ws.cell(row=current_row, column=col)
        cell.value = employee_type
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='center')
        # Color code: Commission employees get yellow background
        if 'commission' in employee_type.lower() or 'comision' in employee_type.lower():
            cell.fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
        type_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # Total Hours
        cell = ws.cell(row=current_row, column=col)
        cell.value = total_hours
        cell.number_format = '0.00' if total_hours > 0 else '0'
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='center')
        hours_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # *** HOURLY RATE - EDITABLE! (pre-filled with minimum wage if empty) ***
        cell = ws.cell(row=current_row, column=col)
        cell.value = employee_rate  # Pre-fill with rate (or min wage if empty)
        cell.number_format = '$#,##0.00'
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='center')
        # GRAY background indicates this is EDITABLE
        cell.fill = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")
        rate_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # *** COMMISSION $ - MANUAL ENTRY FIELD (editable) ***
        cell = ws.cell(row=current_row, column=col)
        cell.value = None  # Leave blank for manual entry
        cell.number_format = '$#,##0.00'
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='right')
        # GRAY background indicates this is an EDITABLE field
        cell.fill = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")
        commission_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # Minimum Wage Guarantee (automatic calculation: hours × min_wage_rate)
        cell = ws.cell(row=current_row, column=col)
        cell.value = f'={hours_cell}*{min_wage_rate}'
        cell.number_format = '$#,##0.00'
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='right')
        min_wage_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # Gross Pay - AUTOMATIC FORMULA with commission logic
        # NOW USES THE EDITABLE RATE CELL!
        cell = ws.cell(row=current_row, column=col)
        
        # Excel formula logic:
        # IF employee type contains "commission" or "comision":
        #   IF commission is entered AND > 0:
        #     Pay MAX(commission, min_wage_guarantee)
        #   ELSE:
        #     Pay min_wage_guarantee (safety net)
        # ELSE (hourly employee):
        #   Pay hours × rate (NOW USES EDITABLE RATE!)
        
        cell.value = (
            f'=IF(OR(ISNUMBER(SEARCH("commission",LOWER({type_cell}))),ISNUMBER(SEARCH("comision",LOWER({type_cell})))),'
            f'IF(AND(ISNUMBER({commission_cell}),{commission_cell}>0),'
            f'MAX({commission_cell},{min_wage_cell}),'
            f'{min_wage_cell}),'
            f'{hours_cell}*{rate_cell})'  # Uses the editable rate cell!
        )
        
        cell.number_format = '$#,##0.00'
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='right')
        cell.font = Font(bold=True)
        gross_pay_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # Payment Type (shows HOW employee was paid)
        cell = ws.cell(row=current_row, column=col)
        cell.value = (
            f'=IF(OR(ISNUMBER(SEARCH("commission",LOWER({type_cell}))),ISNUMBER(SEARCH("comision",LOWER({type_cell})))),'
            f'IF(AND(ISNUMBER({commission_cell}),{commission_cell}>0),'
            f'IF({commission_cell}>{min_wage_cell},"Commission","Hourly (Min Wage)"),'
            f'"Hourly (Min Wage)"),'
            f'"Hourly")'
        )
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='center')
        cell.font = Font(size=9)
        
        current_row += 1
    
    # Set column widths for readability
    ws.column_dimensions['A'].width = 12   # Employee ID
    ws.column_dimensions['B'].width = 25   # Name
    ws.column_dimensions['C'].width = 15   # Type
    ws.column_dimensions['D'].width = 12   # Hours
    ws.column_dimensions['E'].width = 12   # Hourly Rate (editable)
    ws.column_dimensions['F'].width = 14   # Commission (editable)
    ws.column_dimensions['G'].width = 12   # Min Wage
    ws.column_dimensions['H'].width = 12   # Gross Pay
    ws.column_dimensions['I'].width = 20   # Payment Type
    
    # TOTALS ROW
    current_row += 1
    ws[f'A{current_row}'] = "TOTALS"
    ws[f'A{current_row}'].font = Font(bold=True, size=11)
    ws.merge_cells(f'A{current_row}:B{current_row}')
    
    # Total Hours (column D)
    cell = ws.cell(row=current_row, column=4)
    cell.value = f'=SUM(D{start_data_row}:D{current_row-2})'
    cell.font = Font(bold=True)
    cell.number_format = '0.00'
    cell.border = Border(top=Side(style='double'), bottom=Side(style='double'))
    cell.alignment = Alignment(horizontal='center')
    
    # Total Commission (column F)
    cell = ws.cell(row=current_row, column=6)
    cell.value = f'=SUMIF(F{start_data_row}:F{current_row-2},">0")'
    cell.font = Font(bold=True)
    cell.number_format = '$#,##0.00'
    cell.border = Border(top=Side(style='double'), bottom=Side(style='double'))
    cell.alignment = Alignment(horizontal='right')
    
    # Total Gross Pay (column H)
    cell = ws.cell(row=current_row, column=8)
    cell.value = f'=SUM(H{start_data_row}:H{current_row-2})'
    cell.font = Font(bold=True)
    cell.number_format = '$#,##0.00'
    cell.border = Border(top=Side(style='double'), bottom=Side(style='double'))
    cell.alignment = Alignment(horizontal='right')
    
    # PAYMENT SUMMARY SECTION
    current_row += 3
    ws[f'A{current_row}'] = "PAYMENT SUMMARY"
    ws[f'A{current_row}'].font = Font(bold=True, size=11, underline='single')
    current_row += 1
    
    # Count employees paid by commission
    ws[f'A{current_row}'] = "Employees paid by commission:"
    ws[f'A{current_row}'].font = Font(size=10)
    cell = ws.cell(row=current_row, column=2)
    cell.value = f'=COUNTIF(I{start_data_row}:I{current_row-5},"Commission")'
    cell.font = Font(bold=True, size=10)
    current_row += 1
    
    # Count commission employees who got min wage instead
    ws[f'A{current_row}'] = "Commission employees paid min wage:"
    ws[f'A{current_row}'].font = Font(size=10)
    cell = ws.cell(row=current_row, column=2)
    cell.value = f'=COUNTIF(I{start_data_row}:I{current_row-6},"Hourly (Min Wage)")'
    cell.font = Font(bold=True, size=10)
    current_row += 1
    
    # Count hourly employees
    ws[f'A{current_row}'] = "Hourly employees:"
    ws[f'A{current_row}'].font = Font(size=10)
    cell = ws.cell(row=current_row, column=2)
    cell.value = f'=COUNTIF(I{start_data_row}:I{current_row-7},"Hourly")'
    cell.font = Font(bold=True, size=10)
    
    return ws
    """
    Sheet 3: Pay calculations with MANUAL commission and hourly rate entry
    
    Features:
    - Gray highlighted column for MANUAL commission entry
    - Gray highlighted column for MANUAL hourly rate entry (editable for raises/changes)
    - Automatic minimum wage guarantee calculation
    - Formula automatically pays MAX(commission, minimum_wage) for commission employees
    - Standard hourly pay for hourly employees
    - Payment type indicator showing how each employee was paid
    
    Args:
        wb: Workbook object
        df: DataFrame with timesheet data
        styles: Dictionary of cell styles
        company_name: Company name for header
        period_text: Pay period text
        hourly_rate: Default hourly rate if not in user data (pre-fills the editable field)
        min_wage_rate: Minimum wage rate (default $8.50 for Puerto Rico)
    """
    ws = wb.create_sheet("Payroll")
    current_row = 1
    
    # Title
    ws[f'A{current_row}'] = f"{company_name} - PAY CALCULATIONS" if company_name else "PAY CALCULATIONS"
    ws[f'A{current_row}'].font = styles['title_font']
    current_row += 1
    
    # Period
    ws[f'A{current_row}'] = f"Pay Period: {period_text}"
    ws[f'A{current_row}'].font = Font(bold=True, size=11)
    current_row += 2
    
    # Min Wage Rate Display (so users know what it is)
    ws[f'A{current_row}'] = f"Current Minimum Wage Rate: ${min_wage_rate:.2f}/hour"
    ws[f'A{current_row}'].font = Font(bold=True, size=10, color="0000FF")
    current_row += 2
    
    # Instructions (highlighted in red for visibility)
    ws[f'A{current_row}'] = "INSTRUCTIONS:"
    ws[f'A{current_row}'].font = Font(bold=True, size=11, color="FF0000")
    current_row += 1
    
    ws[f'A{current_row}'] = "1. GRAY columns are EDITABLE - Update hourly rates if employee got a raise"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 1
    
    ws[f'A{current_row}'] = "2. For COMMISSION employees: Enter total commission $ in the gray 'Commission $' column"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 1
    
    ws[f'A{current_row}'] = f"3. System compares commission vs minimum wage guarantee (${min_wage_rate}/hr × hours worked)"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 1
    
    ws[f'A{current_row}'] = "4. Commission employees receive whichever is HIGHER (commission or minimum wage)"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 1
    
    ws[f'A{current_row}'] = "5. HOURLY employees are paid: Hours × Hourly Rate (commission column is ignored)"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 1
    
    ws[f'A{current_row}'] = "6. All calculations update automatically when you change rates or commission amounts"
    ws[f'A{current_row}'].font = Font(italic=True, size=9)
    current_row += 2
    
    # Headers
    headers = [
        'Employee ID', 
        'Employee Name', 
        'Employee Type',
        'Total Hours', 
        'Hourly Rate',
        'Commission $',
        'Min Wage\nGuarantee',
        'Gross Pay',
        'Payment Type'
    ]
    
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=current_row, column=col_num)
        cell.value = header
        cell.font = styles['header_font']
        cell.fill = styles['header_fill']
        cell.alignment = Alignment(horizontal='center', wrap_text=True, vertical='center')
        cell.border = styles['border']
    
    current_row += 1
    start_data_row = current_row
    
    # Column references
    employee_col = 'employeeIdVal' if 'employeeIdVal' in df.columns else 'id'
    name_col = 'users_fullName' if 'users_fullName' in df.columns else 'Unknown'
    pay_rate_col = 'users_payRate' if 'users_payRate' in df.columns else None
    employee_type_col = 'users_employeeType' if 'users_employeeType' in df.columns else None
    
    # Process each employee
    for employee_id in df[employee_col].unique():
        employee_df = df[df[employee_col] == employee_id]
        employee_name = employee_df[name_col].iloc[0] if name_col in df.columns else 'Unknown'
        total_hours = employee_df['shiftHoursWorked'].sum() if 'shiftHoursWorked' in df.columns else 0
        
        # Get employee type (default to Hourly if not specified)
        if employee_type_col and employee_type_col in df.columns and pd.notna(employee_df[employee_type_col].iloc[0]):
            employee_type = str(employee_df[employee_type_col].iloc[0])
        else:
            employee_type = 'Hourly'  # Default assumption
        
        # Get hourly rate (this will PRE-FILL the editable field)
        if pay_rate_col and pay_rate_col in df.columns and pd.notna(employee_df[pay_rate_col].iloc[0]):
            employee_rate = float(employee_df[pay_rate_col].iloc[0])
        elif hourly_rate:
            employee_rate = hourly_rate
        else:
            employee_rate = min_wage_rate  # Default to minimum wage
        
        col = 1
        
        # Employee ID
        ws.cell(row=current_row, column=col).value = employee_id
        ws.cell(row=current_row, column=col).border = styles['border']
        ws.cell(row=current_row, column=col).alignment = Alignment(horizontal='center')
        col += 1
        
        # Employee Name
        ws.cell(row=current_row, column=col).value = employee_name
        ws.cell(row=current_row, column=col).border = styles['border']
        col += 1
        
        # Employee Type
        cell = ws.cell(row=current_row, column=col)
        cell.value = employee_type
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='center')
        # Color code: Commission employees get yellow background
        if 'commission' in employee_type.lower() or 'comision' in employee_type.lower():
            cell.fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
        type_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # Total Hours
        cell = ws.cell(row=current_row, column=col)
        cell.value = total_hours
        cell.number_format = '0.00' if total_hours > 0 else '0'
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='center')
        hours_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # *** HOURLY RATE - NOW EDITABLE! (pre-filled with value from Noloco) ***
        cell = ws.cell(row=current_row, column=col)
        cell.value = employee_rate  # Pre-fill with current rate
        cell.number_format = '$#,##0.00'
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='center')
        # GRAY background indicates this is EDITABLE
        cell.fill = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")
        rate_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # *** COMMISSION $ - MANUAL ENTRY FIELD (editable) ***
        cell = ws.cell(row=current_row, column=col)
        cell.value = None  # Leave blank for manual entry
        cell.number_format = '$#,##0.00'
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='right')
        # GRAY background indicates this is an EDITABLE field
        cell.fill = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")
        commission_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # Minimum Wage Guarantee (automatic calculation: hours × min_wage_rate)
        cell = ws.cell(row=current_row, column=col)
        cell.value = f'={hours_cell}*{min_wage_rate}'
        cell.number_format = '$#,##0.00'
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='right')
        min_wage_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # Gross Pay - AUTOMATIC FORMULA with commission logic
        # NOW USES THE EDITABLE RATE CELL!
        cell = ws.cell(row=current_row, column=col)
        
        # Excel formula logic:
        # IF employee type contains "commission" or "comision":
        #   IF commission is entered AND > 0:
        #     Pay MAX(commission, min_wage_guarantee)
        #   ELSE:
        #     Pay min_wage_guarantee (safety net)
        # ELSE (hourly employee):
        #   Pay hours × rate (NOW USES EDITABLE RATE!)
        
        cell.value = (
            f'=IF(OR(ISNUMBER(SEARCH("commission",LOWER({type_cell}))),ISNUMBER(SEARCH("comision",LOWER({type_cell})))),'
            f'IF(AND(ISNUMBER({commission_cell}),{commission_cell}>0),'
            f'MAX({commission_cell},{min_wage_cell}),'
            f'{min_wage_cell}),'
            f'{hours_cell}*{rate_cell})'  # Uses the editable rate cell!
        )
        
        cell.number_format = '$#,##0.00'
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='right')
        cell.font = Font(bold=True)
        gross_pay_cell = f'{get_column_letter(col)}{current_row}'
        col += 1
        
        # Payment Type (shows HOW employee was paid)
        cell = ws.cell(row=current_row, column=col)
        cell.value = (
            f'=IF(OR(ISNUMBER(SEARCH("commission",LOWER({type_cell}))),ISNUMBER(SEARCH("comision",LOWER({type_cell})))),'
            f'IF(AND(ISNUMBER({commission_cell}),{commission_cell}>0),'
            f'IF({commission_cell}>{min_wage_cell},"Commission","Hourly (Min Wage)"),'
            f'"Hourly (Min Wage)"),'
            f'"Hourly")'
        )
        cell.border = styles['border']
        cell.alignment = Alignment(horizontal='center')
        cell.font = Font(size=9)
        
        current_row += 1
    
    # Set column widths for readability
    ws.column_dimensions['A'].width = 12   # Employee ID
    ws.column_dimensions['B'].width = 25   # Name
    ws.column_dimensions['C'].width = 15   # Type
    ws.column_dimensions['D'].width = 12   # Hours
    ws.column_dimensions['E'].width = 12   # Hourly Rate (editable)
    ws.column_dimensions['F'].width = 14   # Commission (editable)
    ws.column_dimensions['G'].width = 12   # Min Wage
    ws.column_dimensions['H'].width = 12   # Gross Pay
    ws.column_dimensions['I'].width = 20   # Payment Type
    
    # TOTALS ROW
    current_row += 1
    ws[f'A{current_row}'] = "TOTALS"
    ws[f'A{current_row}'].font = Font(bold=True, size=11)
    ws.merge_cells(f'A{current_row}:B{current_row}')
    
    # Total Hours (column D)
    cell = ws.cell(row=current_row, column=4)
    cell.value = f'=SUM(D{start_data_row}:D{current_row-2})'
    cell.font = Font(bold=True)
    cell.number_format = '0.00'
    cell.border = Border(top=Side(style='double'), bottom=Side(style='double'))
    cell.alignment = Alignment(horizontal='center')
    
    # Total Commission (column F)
    cell = ws.cell(row=current_row, column=6)
    cell.value = f'=SUMIF(F{start_data_row}:F{current_row-2},">0")'
    cell.font = Font(bold=True)
    cell.number_format = '$#,##0.00'
    cell.border = Border(top=Side(style='double'), bottom=Side(style='double'))
    cell.alignment = Alignment(horizontal='right')
    
    # Total Gross Pay (column H)
    cell = ws.cell(row=current_row, column=8)
    cell.value = f'=SUM(H{start_data_row}:H{current_row-2})'
    cell.font = Font(bold=True)
    cell.number_format = '$#,##0.00'
    cell.border = Border(top=Side(style='double'), bottom=Side(style='double'))
    cell.alignment = Alignment(horizontal='right')
    
    # PAYMENT SUMMARY SECTION
    current_row += 3
    ws[f'A{current_row}'] = "PAYMENT SUMMARY"
    ws[f'A{current_row}'].font = Font(bold=True, size=11, underline='single')
    current_row += 1
    
    # Count employees paid by commission
    ws[f'A{current_row}'] = "Employees paid by commission:"
    ws[f'A{current_row}'].font = Font(size=10)
    cell = ws.cell(row=current_row, column=2)
    cell.value = f'=COUNTIF(I{start_data_row}:I{current_row-5},"Commission")'
    cell.font = Font(bold=True, size=10)
    current_row += 1
    
    # Count commission employees who got min wage instead
    ws[f'A{current_row}'] = "Commission employees paid min wage:"
    ws[f'A{current_row}'].font = Font(size=10)
    cell = ws.cell(row=current_row, column=2)
    cell.value = f'=COUNTIF(I{start_data_row}:I{current_row-6},"Hourly (Min Wage)")'
    cell.font = Font(bold=True, size=10)
    current_row += 1
    
    # Count hourly employees
    ws[f'A{current_row}'] = "Hourly employees:"
    ws[f'A{current_row}'].font = Font(size=10)
    cell = ws.cell(row=current_row, column=2)
    cell.value = f'=COUNTIF(I{start_data_row}:I{current_row-7},"Hourly")'
    cell.font = Font(bold=True, size=10)
    
    return ws


# USAGE EXAMPLE:
# In your main payroll script, call this function like:
 create_pay_calculations_sheet(
     wb=wb, 
     df=df, 
     styles=styles, 
     company_name="Pet Esthetic",
     period_text=period_text,
     hourly_rate=None,  # Will use rates from user profiles
     min_wage_rate=10.50  # Puerto Rico minimum wage
 )

import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from io import StringIO
import pandas as pd
from dotenv import load_dotenv


def send_gmail(
    to_emails,
    subject,
    body_html="",
    from_email=None,
    app_password=None,
    df_attachment=None,
    attachment_filename="data.csv"
):
    """
    Send an email using Gmail SMTP with App Password
    
    Args:
        to_emails: String (single email) or List of email addresses
        subject: Email subject line
        body_html: Email body content in HTML format (optional)
        from_email: Gmail address (optional, reads from env GMAIL_EMAIL)
        app_password: Gmail App Password (optional, reads from env GMAIL_APP_PASSWORD)
        df_attachment: pandas DataFrame to attach as CSV (optional)
        attachment_filename: Name for the CSV attachment (default: "data.csv")
        
    Returns:
        True if email sent successfully, False otherwise
        
    Example:
        # Simple email
        send_gmail(
            to_emails="recipient@example.com",
            subject="Test Email",
            body_html="<h1>Hello!</h1><p>This is a test.</p>"
        )
        
        # Multiple recipients with DataFrame attachment
        send_gmail(
            to_emails=["person1@example.com", "person2@example.com"],
            subject="Weekly Report",
            body_html="<p>Please see attached data.</p>",
            df_attachment=my_dataframe,
            attachment_filename="weekly_report.csv"
        )
    """
    
    # Get credentials from environment variables if not provided
    if from_email is None:
        from_email = os.getenv('GMAIL_EMAIL')
    if app_password is None:
        app_password = os.getenv('GMAIL_APP_PASSWORD')
    
    # Validate credentials
    if not from_email:
        raise Exception("Gmail email not provided. Set GMAIL_EMAIL environment variable or pass from_email parameter.")
    if not app_password:
        raise Exception("Gmail App Password not provided. Set GMAIL_APP_PASSWORD environment variable or pass app_password parameter.")
    
    # Handle single email string or list of emails
    if isinstance(to_emails, str):
        to_emails = [to_emails]
    
    if not to_emails or len(to_emails) == 0:
        raise Exception("At least one recipient email address is required.")
    
    # Validate email addresses
    for email in to_emails:
        if '@' not in email:
            raise Exception(f"Invalid email address: {email}")
    
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = from_email
        msg['To'] = ', '.join(to_emails)  # Join multiple emails with comma
        
        # Attach HTML body if provided
        if body_html:
            html_part = MIMEText(body_html, 'html')
            msg.attach(html_part)
        
        # Attach DataFrame as CSV if provided
        if df_attachment is not None:
            if not isinstance(df_attachment, pd.DataFrame):
                raise Exception("df_attachment must be a pandas DataFrame")
            
            # Convert DataFrame to CSV string
            csv_buffer = StringIO()
            df_attachment.to_csv(csv_buffer, index=False)
            csv_string = csv_buffer.getvalue()
            
            # Create attachment
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(csv_string.encode('utf-8'))
            encoders.encode_base64(attachment)
            attachment.add_header(
                'Content-Disposition',
                f'attachment; filename="{attachment_filename}"'
            )
            msg.attach(attachment)
        
        # Connect to Gmail SMTP server
        print(f"  Connecting to Gmail SMTP server...")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()  # Enable TLS encryption
        
        # Login with app password
        print(f"  Logging in as {from_email}...")
        server.login(from_email, app_password)
        
        # Send email
        recipient_list = ', '.join(to_emails)
        print(f"  Sending email to {recipient_list}...")
        server.send_message(msg)
        
        # Close connection
        server.quit()
        
        print(f"  ✓ Email sent successfully!")
        return True
        
    except smtplib.SMTPAuthenticationError as e:
        raise Exception(f"Gmail authentication failed. Check your GMAIL_EMAIL and GMAIL_APP_PASSWORD. Error: {str(e)}")
    
    except smtplib.SMTPException as e:
        raise Exception(f"SMTP error occurred: {str(e)}")
    
    except Exception as e:
        raise Exception(f"Failed to send email: {str(e)}")

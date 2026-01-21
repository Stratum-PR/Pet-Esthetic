import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from io import StringIO
import pandas as pd


def send_gmail(
    to_emails,
    subject,
    body_html="",
    from_email=None,
    app_password=None,
    attachment_path=None,
    attachment_filename=None
):
    """
    Send an email using Gmail SMTP with App Password
    
    Args:
        to_emails: String (single email) or List of email addresses
        subject: Email subject line
        body_html: Email body content in HTML format (optional)
        from_email: Gmail address (optional, reads from env GMAIL_EMAIL)
        app_password: Gmail App Password (optional, reads from env GMAIL_APP_PASSWORD)
        attachment_path: Path to file to attach (supports .csv, .xlsx, .zip, etc)
        attachment_filename: Name for the attachment (optional, uses filename from path)
        
    Returns:
        True if email sent successfully, False otherwise
        
    Example:
        # Simple email
        send_gmail(
            to_emails="recipient@example.com",
            subject="Test Email",
            body_html="<h1>Hello!</h1><p>This is a test.</p>"
        )
        
        # With Excel attachment
        send_gmail(
            to_emails=["person1@example.com", "person2@example.com"],
            subject="Weekly Report",
            body_html="<p>Please see attached data.</p>",
            attachment_path="/path/to/report.xlsx",
            attachment_filename="weekly_report.xlsx"
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
        
        # Attach file if provided
        if attachment_path and os.path.exists(attachment_path):
            # Determine filename
            if not attachment_filename:
                attachment_filename = os.path.basename(attachment_path)
            
            # Read file
            with open(attachment_path, 'rb') as f:
                file_data = f.read()
            
            # Determine MIME type based on extension
            file_ext = os.path.splitext(attachment_path)[1].lower()
            if file_ext == '.xlsx':
                mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            elif file_ext == '.csv':
                mime_type = 'text/csv'
            elif file_ext == '.zip':
                mime_type = 'application/zip'
            elif file_ext == '.pdf':
                mime_type = 'application/pdf'
            else:
                mime_type = 'application/octet-stream'
            
            # Create attachment
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(file_data)
            encoders.encode_base64(attachment)
            attachment.add_header(
                'Content-Disposition',
                f'attachment; filename="{attachment_filename}"'
            )
            attachment.add_header('Content-Type', mime_type)
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


# Test function to verify email setup
def test_gmail_connection():
    """
    Test Gmail connection without sending an email
    Use this to verify your Gmail credentials are set up correctly
    """
    print("Testing Gmail connection...")
    print("=" * 60)
    
    try:
        # Get credentials
        from_email = os.getenv('GMAIL_EMAIL')
        app_password = os.getenv('GMAIL_APP_PASSWORD')
        
        if not from_email:
            print("✗ GMAIL_EMAIL environment variable not set!")
            return False
        if not app_password:
            print("✗ GMAIL_APP_PASSWORD environment variable not set!")
            return False
        
        print(f"✓ Gmail Email: {from_email}")
        print(f"✓ App Password: {'*' * len(app_password)} (set)")
        print()
        
        # Test connection without sending
        print("  Connecting to Gmail SMTP server...")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        
        print("  Testing authentication...")
        server.login(from_email, app_password)
        
        print("  ✓ Authentication successful!")
        server.quit()
        
        print()
        print("=" * 60)
        print("✓ SUCCESS! Gmail connection validated.")
        print("  Credentials are correct and ready to send emails.")
        print("=" * 60)
        return True
        
    except smtplib.SMTPAuthenticationError as e:
        print()
        print("=" * 60)
        print(f"✗ AUTHENTICATION ERROR: {str(e)}")
        print("=" * 60)
        print("\nTroubleshooting:")
        print("1. Make sure 2-Factor Authentication is enabled on your Gmail")
        print("2. Generate an App Password at: https://myaccount.google.com/apppasswords")
        print("3. Check environment variables:")
        print("   GMAIL_EMAIL=your.email@gmail.com")
        print("   GMAIL_APP_PASSWORD=your16charpassword")
        return False
    
    except Exception as e:
        print()
        print("=" * 60)
        print(f"✗ ERROR: {str(e)}")
        print("=" * 60)
        print("\nTroubleshooting:")
        print("1. Check your internet connection")
        print("2. Verify Gmail SMTP is accessible (not blocked by firewall)")
        print("3. Confirm credentials are set correctly")
        return False


# Run test if executed directly
if __name__ == "__main__":
    test_gmail_connection()

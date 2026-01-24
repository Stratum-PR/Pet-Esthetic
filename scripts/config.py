"""
Configuration module for Noloco Timesheet Sync
Centralizes all configuration settings and environment variables
"""
import os
from dataclasses import dataclass
from typing import List

@dataclass
class Config:
    """Configuration settings for timesheet sync"""
    # API Configuration
    api_token: str
    project_id: str
    api_url: str
    
    # Email Configuration
    email_recipients: List[str]
    gmail_email: str = None
    gmail_app_password: str = None
    
    # Retry Configuration
    max_retries: int = 3
    retry_delay: int = 2
    rate_limit_delay: float = 0.5
    
    # Timeout Configuration
    request_timeout: int = 30
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables"""
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
        api_token = os.getenv('NOLOCO_API_TOKEN')
        project_id = os.getenv('NOLOCO_PROJECT_ID')
        
        if not api_token:
            raise Exception("ERROR: NOLOCO_API_TOKEN environment variable not set!")
        if not project_id:
            raise Exception("ERROR: NOLOCO_PROJECT_ID environment variable not set!")
        
        # Parse email recipients
        recipients_str = os.getenv('EMAIL_RECIPIENTS', '')
        email_recipients = [email.strip() for email in recipients_str.split(',') if email.strip()]
        
        return cls(
            api_token=api_token,
            project_id=project_id,
            api_url=f"https://api.portals.noloco.io/data/{project_id}",
            email_recipients=email_recipients,
            gmail_email=os.getenv('GMAIL_EMAIL'),
            gmail_app_password=os.getenv('GMAIL_APP_PASSWORD')
        )
    
    @property
    def headers(self):
        """Get HTTP headers for API requests"""
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
    
    def validate_email_config(self):
        """Check if email configuration is valid"""
        if not self.email_recipients:
            return False, "EMAIL_RECIPIENTS not configured"
        if not self.gmail_email:
            return False, "GMAIL_EMAIL not configured"
        if not self.gmail_app_password:
            return False, "GMAIL_APP_PASSWORD not configured"
        return True, "Email configuration valid"

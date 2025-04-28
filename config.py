"""
Configuration settings for the invoice collection application
"""

# Google API scopes
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.settings.basic",
    "https://www.googleapis.com/auth/gmail.settings.sharing",
    "https://mail.google.com/",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/drive.appdata",
    "https://www.googleapis.com/auth/drive.metadata",
    "https://www.googleapis.com/auth/drive.photos.readonly",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/bigquery.insertdata",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/cloud-platform.read-only",
]

# Email settings
EMAIL_QUERY = "from:duyloc91@gmail.com label:Email Test"
MAX_EMAILS = 1000

# BigQuery settings
BQ_DATASET = "finance_project"
BQ_TABLE = "email_summaries"

# File paths
CREDENTIALS_FILE = "finance_app_oauth.json"
TOKEN_FILE = "finance_app_token.pq"
OPENAI = "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"
DRIVE = "https://drive.google.com/drive/folders/10_TscWRXUYrTjgaE9LF-MxI53x3q60oh"
# Report generator settings
REPORT_DAYS = 30  # Number of days of data to include
REPORT_FOLDER = "company_reports"  # Output folder
REPORT_TEMPLATE = "path/to/template.xlsx"  # Template file (optional)

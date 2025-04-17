"""
Configuration settings for the invoice collection application
"""

# Google API scopes
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/bigquery",
]

# Email settings
EMAIL_QUERY = "from:your-partner-email@example.com has:attachment"
MAX_EMAILS = 10

# BigQuery settings
BQ_DATASET = "invoice_dataset"
BQ_TABLE = "email_summaries"

# File paths
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.pickle"

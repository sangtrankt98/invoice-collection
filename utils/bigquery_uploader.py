"""
Module for uploading data to BigQuery
"""

import json
from datetime import datetime
from google.cloud import bigquery
from google.oauth2.credentials import Credentials


class BigQueryUploader:
    """Handles BigQuery operations"""

    def __init__(self, credentials):
        """Initialize with Google credentials"""
        self.credentials = credentials
        # Create BigQuery client from OAuth credentials
        self.client = bigquery.Client(credentials=credentials)

    def upload_data(self, email_data, dataset_id, table_id):
        """Upload the email data to BigQuery"""
        table_ref = self.client.dataset(dataset_id).table(table_id)

        # Prepare data for BigQuery
        rows_to_insert = []
        for email in email_data:
            row = {
                "message_id": email["message_id"],
                "date": email["date"],
                "subject": email["subject"],
                "from_email": email["from"],
                "summary": email["summary"],
                "drive_links": json.dumps(email["drive_links"]),
                "attachment_count": len(email["attachments"]),
                "attachment_details": json.dumps(
                    [
                        {
                            "filename": att["filename"],
                            "mime_type": att["mime_type"],
                            "size": att["size"],
                        }
                        for att in email["attachments"]
                    ]
                ),
                "processed_date": datetime.now().isoformat(),
            }
            rows_to_insert.append(row)

        # Insert data
        errors = self.client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")

        return errors

    def create_table_if_not_exists(self, dataset_id, table_id):
        """Create BigQuery table if it doesn't exist"""
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        try:
            self.client.get_table(table_ref)
            print(f"Table {dataset_id}.{table_id} already exists.")
        except Exception:
            # Table does not exist, create it
            schema = [
                bigquery.SchemaField("message_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date", "STRING"),
                bigquery.SchemaField("subject", "STRING"),
                bigquery.SchemaField("from_email", "STRING"),
                bigquery.SchemaField("summary", "STRING"),
                bigquery.SchemaField("drive_links", "STRING"),
                bigquery.SchemaField("attachment_count", "INTEGER"),
                bigquery.SchemaField("attachment_details", "STRING"),
                bigquery.SchemaField("processed_date", "TIMESTAMP"),
            ]

            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            print(f"Created table {dataset_id}.{table_id}")

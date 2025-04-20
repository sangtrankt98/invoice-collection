"""
Module for uploading data to BigQuery
"""

import json
import logging
from datetime import datetime
from google.cloud import bigquery
from google.oauth2.credentials import Credentials

# Set up logger
logger = logging.getLogger("invoice_collection.bigquery")


class BigQueryUploader:
    """Handles BigQuery operations"""

    def __init__(self, credentials):
        """Initialize with Google credentials"""
        logger.info("Initializing BigQuery uploader")
        self.credentials = credentials
        # Create BigQuery client from OAuth credentials
        try:
            self.client = bigquery.Client(
                credentials=credentials, project="immortal-0804"
            )
            logger.info("BigQuery client created successfully")
        except Exception as e:
            logger.error(f"Error creating BigQuery client: {e}")
            raise

    def upload_data(self, email_data, dataset_id, table_id):
        """Upload the email data to BigQuery"""
        logger.info(f"Preparing to upload data to {dataset_id}.{table_id}")

        # Ensure the table exists
        self.create_table_if_not_exists(dataset_id, table_id)

        table_ref = self.client.dataset(dataset_id).table(table_id)

        # Prepare data for BigQuery
        rows_to_insert = []
        for email in email_data:
            try:
                row = {
                    "message_id": email["message_id"],
                    "date": email["date"],
                    "subject": email["subject"],
                    "from_email": email["from"],
                    "summary": email["summary"],
                    "attachment_count": len(email["attachments"]),
                    "attachment_details": [
                        {
                            "filename": att["filename"],
                            "mime_type": att["mime_type"],
                            "size": att["size"],
                            "gdrive_link": att["gdrive_link"],
                            "company_name": att["company_name"],
                            "company_tax_number": att["company_tax_number"],
                            "seller": att["seller"],
                            "date": att["date"],
                            "invoice_number": att["invoice_number"],
                            "total_amount": att["total_amount"],
                        }
                        for att in email["attachments"]
                    ],
                    "processed_date": datetime.now().isoformat(),
                }
                rows_to_insert.append(row)
                logger.debug(f"Prepared row for message ID: {email['message_id']}")
            except Exception as e:
                logger.error(
                    f"Error preparing row for message ID {email['message_id']}: {e}"
                )

        logger.info(f"Uploading {len(rows_to_insert)} rows to BigQuery")

        # Insert data
        try:
            errors = self.client.insert_rows_json(table_ref, rows_to_insert)
            if errors:
                for error in errors:
                    logger.error(f"BigQuery insertion error: {error}")
            else:
                logger.info(
                    f"Successfully inserted {len(rows_to_insert)} rows to BigQuery"
                )
            return errors
        except Exception as e:
            logger.error(f"Error uploading to BigQuery: {e}")
            return [str(e)]

    def create_table_if_not_exists(self, dataset_id, table_id):
        """Create BigQuery table if it doesn't exist"""
        logger.info(f"Checking if table {dataset_id}.{table_id} exists")
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {dataset_id}.{table_id} already exists")
        except Exception:
            logger.info(f"Table {dataset_id}.{table_id} does not exist, creating it")
            # Create the dataset if it doesn't exist
            try:
                dataset = self.client.create_dataset(dataset_id, exists_ok=True)
                logger.info(f"Dataset {dataset_id} ensured")
            except Exception as e:
                logger.error(f"Error creating dataset {dataset_id}: {e}")
                raise

            # Create the table
            schema = [
                bigquery.SchemaField("message_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date", "STRING"),
                bigquery.SchemaField("subject", "STRING"),
                bigquery.SchemaField("from_email", "STRING"),
                bigquery.SchemaField("summary", "STRING"),
                bigquery.SchemaField("attachment_count", "INTEGER"),
                bigquery.SchemaField(
                    "attachment_details",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("filename", "STRING"),
                        bigquery.SchemaField("mime_type", "STRING"),
                        bigquery.SchemaField("size", "INTEGER"),
                        bigquery.SchemaField("gdrive_link", "STRING"),
                        bigquery.SchemaField("company_name", "STRING"),
                        bigquery.SchemaField("company_tax_number", "STRING"),
                        bigquery.SchemaField("seller", "STRING"),
                        bigquery.SchemaField("date", "DATE"),
                        bigquery.SchemaField("invoice_number", "STRING"),
                        bigquery.SchemaField("total_amount", "FLOAT"),
                    ],
                ),
                bigquery.SchemaField("processed_date", "TIMESTAMP"),
            ]

            try:
                table = bigquery.Table(table_ref, schema=schema)
                table = self.client.create_table(table)
                logger.info(f"Created table {dataset_id}.{table_id}")
            except Exception as e:
                logger.error(f"Error creating table {dataset_id}.{table_id}: {e}")
                raise

    def extract_data(self, email_data):
        """Upload the email data to BigQuery"""
        logger.info(f"Preparing data")
        # Prepare data
        rows_to_insert = []
        for email in email_data:
            try:
                row = {
                    "message_id": email["message_id"],
                    "date": email["date"],
                    "subject": email["subject"],
                    "from_email": email["from"],
                    "summary": email["summary"],
                    "attachment_count": len(email["attachments"]),
                    "attachment_details": [
                        {
                            "filename": att["filename"],
                            "mime_type": att["mime_type"],
                            "size": att["size"],
                            "gdrive_link": att["gdrive_link"],
                            "company_name": att["company_name"],
                            "company_tax_number": att["company_tax_number"],
                            "seller": att["seller"],
                            "date": att["date"],
                            "invoice_number": att["invoice_number"],
                            "total_amount": att["total_amount"],
                        }
                        for att in email["attachments"]
                    ],
                    "processed_date": datetime.now().isoformat(),
                }
                rows_to_insert.append(row)
                logger.debug(f"Prepared row for message ID: {email['message_id']}")
            except Exception as e:
                logger.error(
                    f"Error preparing row for message ID {email['message_id']}: {e}"
                )
        logger.info(f"Done generating data")
        return rows_to_insert

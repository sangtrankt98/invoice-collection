"""
Module for uploading data to BigQuery
"""

import json
import logging
from datetime import datetime
from google.cloud import bigquery
from pandas import DataFrame

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

    def upload_dataframe(
        self, data, project_id, dataset_id, table_id, if_exists="fail"
    ):
        """
        Upload a Pandas DataFrame to Google BigQuery.

        Parameters:
        -----------
        data : pandas.DataFrame
            The DataFrame to upload to BigQuery
        project_id : str
            Google Cloud project ID
        dataset_id : str
            BigQuery dataset ID
        table_id : str
            BigQuery table ID
        if_exists : str, default 'fail'
            Action to take if the table already exists.
            Options: 'fail', 'replace', 'append'

        Returns:
        --------
        bool
            True if successful, raises an exception otherwise
        """
        try:
            # Validate inputs
            if not isinstance(data, DataFrame):
                raise TypeError("dataframe must be a pandas DataFrame")
            if not all(isinstance(x, str) for x in [project_id, dataset_id, table_id]):
                raise TypeError("project_id, dataset_id, and table_id must be strings")
            if if_exists not in ["fail", "replace", "append"]:
                raise ValueError(
                    "if_exists must be one of: 'fail', 'replace', 'append'"
                )

            # Create full table reference
            table_ref = f"{project_id}.{dataset_id}.{table_id}"

            # Set job configuration
            job_config = bigquery.LoadJobConfig()

            # Set write disposition based on if_exists parameter
            if if_exists == "replace":
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            elif if_exists == "append":
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            else:  # 'fail' is default
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

            # Auto-detect schema
            job_config.autodetect = True

            # Execute load job
            job = self.client.load_table_from_dataframe(
                data, table_ref, job_config=job_config
            )

            # Wait for job to complete
            job.result()

            logger.info(f"Loaded {len(data)} rows into {table_ref}")
            return True

        except Exception as e:
            logger.info(f"Error uploading DataFrame to BigQuery: {str(e)}")
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
                    "email_date": email["date"],
                    "subject": email["subject"],
                    "from_email": email["from"],
                    # "summary": email["summary"],
                    "attachment_count": len(email["attachments"]),
                    "attachment_details": [
                        {
                            "file_origin": att["filename"],
                            "gdrive_link": att["gdrive_link"],
                            "file_naming": att["file_name"],
                            "processed": att["processed"],
                            "skipped": att["skipped"],
                            "error": att["error"],
                            "company_name": att["company_name"],
                            "company_tax_number": att["company_tax_number"],
                            "seller": att["seller"],
                            "invoice_date": att["date"],
                            "invoice_number": att["invoice_number"],
                            "total_before_tax": att["total_before_tax"],
                            "total_tax": att["total_tax"],
                            "total_amount": att["total_amount"],
                        }
                        for att in email["attachments"]
                    ],
                    "processed_date": datetime.now().strftime("%Y-%m-%dT%H:00"),
                }
                rows_to_insert.append(row)
                logger.debug(f"Prepared row for message ID: {email['message_id']}")
            except Exception as e:
                logger.error(
                    f"Error preparing row for message ID {email['message_id']}: {e}"
                )
        logger.info(f"Done generating data")
        return rows_to_insert

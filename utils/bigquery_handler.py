"""
Module for uploading data to BigQuery
"""

import json
import numpy as np
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery
from pandas import DataFrame

# Set up logger
logger = logging.getLogger("invoice_collection.bigquery")


class BigQueryHandler:
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

    def clean_dataframe_for_bigquery(self, df):
        # Columns that should be numeric
        numeric_columns = ["total_before_tax", "total_tax", "total_amount"]

        for col in numeric_columns:
            if col in df.columns:
                # Replace empty strings with NaN
                df[col] = df[col].replace("", np.nan)
                # Convert to float
                df[col] = df[col].astype(float)

        return df

    def extract_data(self, email_data):
        """Upload the email data to BigQuery"""
        logger.info(f"Preparing data")
        # Prepare data
        rows_to_insert = []
        for email in email_data:
            try:
                row = {
                    "message_id": email["message_id"],
                    "thread_id": email["thread_id"],
                    "email_date": email["date"],
                    "internal_date": email["internal_date"],
                    "subject": email["subject"],
                    "from_email": email["from"],
                    "summary": email["summary"],
                    "attachment_count": len(email["attachments"]),
                    "attachment_details": [
                        {
                            "file_origin": att["filename"],
                            "file_naming": att["file_name"],
                            "file_type": att["file_type"],
                            "processed": att["processed"],
                            "skipped": att["skipped"],
                            "error": att["error"],
                            "document_type": att["document_type"],
                            "document_number": att["document_number"],
                            "date": att["date"],
                            "entity_name": att["entity_name"],
                            "entity_tax_number": att["entity_tax_number"],
                            "counterparty_name": att["counterparty_name"],
                            "counterparty_tax_number": att["counterparty_tax_number"],
                            "payment_method": att["payment_method"],
                            "amount_before_tax": att["amount_before_tax"],
                            "tax_rate": att["tax_rate"],
                            "tax_amount": att["tax_amount"],
                            "total_amount": att["total_amount"],
                            "direction": att["direction"],
                            "description": att["description"],
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

    def query_transactions_by_date(
        self,
        start_date=None,
        end_date=None,
        entity_name=None,
        project_id="immortal-0804",
        dataset_id="finance_project",
        table_id="invoice_summarize",
    ):
        """
        Query transaction data from BigQuery by date range and optional entity name

        Parameters:
        -----------
        start_date : str, optional
            Start date in format 'YYYY-MM-DD'
        end_date : str, optional
            End date in format 'YYYY-MM-DD'
        entity_name : str, optional
            Filter by specific entity name
        project_id : str
            Google Cloud project ID
        dataset_id : str
            BigQuery dataset ID
        table_id : str
            BigQuery table ID

        Returns:
        --------
        pandas.DataFrame
            DataFrame containing the query results
        """
        try:
            # Default to the past 30 days if no dates provided
            if not start_date:
                end_date_obj = datetime.now()
                start_date_obj = end_date_obj - timedelta(days=30)
                start_date = start_date_obj.strftime("%Y-%m-%d")
                end_date = end_date_obj.strftime("%Y-%m-%d")
            elif not end_date:
                # If only start date is provided, set end date to today
                end_date = datetime.now().strftime("%Y-%m-%d")

            # Build the query
            query = f"""
            SELECT DISTINCT *
            FROM `{project_id}.{dataset_id}.{table_id}`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND document_type = 'INVOICE'
            """

            # Add entity filter if provided
            if entity_name:
                query += f" AND entity_name = '{entity_name}'"

            logger.info(f"Executing query: {query}")

            # Execute the query
            df = self.client.query(query).to_dataframe()

            logger.info(f"Query returned {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"Error querying BigQuery: {str(e)}")
            raise

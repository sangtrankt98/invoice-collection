"""
Module for managing data storage in local files instead of BigQuery
Handles data extraction, storage, and duplicate checking
"""

import json
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import logging
from utils.logger_setup import setup_logger

logger = setup_logger()


class LocalStorageHandler:
    """Handles local storage operations instead of BigQuery"""

    def __init__(self, storage_dir="data_storage"):
        """Initialize with storage directory"""
        logger.info("Initializing Local Storage handler")
        self.storage_dir = storage_dir
        self.master_file = os.path.join(storage_dir, "master_data.csv")
        self.threads_file = os.path.join(storage_dir, "processed_threads.csv")

        # Create storage directory if it doesn't exist
        os.makedirs(storage_dir, exist_ok=True)

        # Initialize master file if it doesn't exist
        if not os.path.exists(self.master_file):
            self._create_empty_master_file()

        # Initialize threads file if it doesn't exist
        if not os.path.exists(self.threads_file):
            self._create_empty_threads_file()

    def _create_empty_master_file(self):
        """Create an empty master file with correct headers"""
        try:
            # Define the columns structure similar to your BigQuery table
            columns = [
                "message_id",
                "thread_id",
                "email_date",
                "internal_date",
                "subject",
                "from_email",
                "summary",
                "attachment_count",
                "file_origin",
                "file_naming",
                "file_type",
                "processed",
                "skipped",
                "error",
                "document_type",
                "document_number",
                "date",
                "entity_name",
                "entity_tax_number",
                "counterparty_name",
                "counterparty_tax_number",
                "payment_method",
                "amount_before_tax",
                "tax_rate",
                "tax_amount",
                "total_amount",
                "direction",
                "description",
                "processed_date",
            ]

            # Create empty DataFrame with correct columns
            df = pd.DataFrame(columns=columns)

            # Save to CSV
            df.to_csv(self.master_file, index=False, encoding="utf-8-sig")
            logger.info(f"Created empty master file at {self.master_file}")
        except Exception as e:
            logger.error(f"Error creating empty master file: {e}")
            raise

    def _create_empty_threads_file(self):
        """Create an empty threads file for tracking processed threads"""
        try:
            # Simple structure with thread_id and processed_date
            df = pd.DataFrame(columns=["thread_id", "processed_date"])
            df.to_csv(self.threads_file, index=False, encoding="utf-8-sig")
            logger.info(f"Created empty threads file at {self.threads_file}")
        except Exception as e:
            logger.error(f"Error creating empty threads file: {e}")
            raise

    def extract_data(self, email_data):
        """
        Extract structured data from email_data
        Similar to the original BigQuery extract_data but prepares for local storage

        Args:
            email_data: List of email data dictionaries

        Returns:
            list: Processed data rows ready for storage
        """
        logger.info(f"Preparing data for local storage")
        rows_to_insert = []

        for email in email_data:
            try:
                # For each attachment, create a separate row in the flattened structure
                for att in email["attachments"]:
                    row = {
                        "message_id": email["message_id"],
                        "thread_id": email["thread_id"],
                        "email_date": email["date"],
                        "internal_date": email["internal_date"],
                        "subject": email["subject"],
                        "from_email": email["from"],
                        "summary": email["summary"],
                        "attachment_count": len(email["attachments"]),
                        "file_origin": att.get("filename", ""),
                        "file_naming": att.get("file_name", ""),
                        "file_type": att.get("file_type", ""),
                        "processed": att.get("processed", False),
                        "skipped": att.get("skipped", False),
                        "error": att.get("error", ""),
                        "document_type": att.get("document_type", None),
                        "document_number": att.get("document_number", None),
                        "date": att.get("date", None),
                        "entity_name": att.get("entity_name", None),
                        "entity_tax_number": att.get("entity_tax_number", None),
                        "counterparty_name": att.get("counterparty_name", None),
                        "counterparty_tax_number": att.get(
                            "counterparty_tax_number", None
                        ),
                        "payment_method": att.get("payment_method", None),
                        "amount_before_tax": att.get("amount_before_tax", None),
                        "tax_rate": att.get("tax_rate", None),
                        "tax_amount": att.get("tax_amount", None),
                        "total_amount": att.get("total_amount", None),
                        "direction": att.get("direction", None),
                        "description": att.get("description", None),
                        "processed_date": datetime.now().strftime("%Y-%m-%dT%H:00"),
                    }
                    rows_to_insert.append(row)

                logger.debug(f"Prepared row for message ID: {email['message_id']}")
            except Exception as e:
                logger.error(
                    f"Error preparing row for message ID {email['message_id']}: {e}"
                )

        logger.info(f"Prepared {len(rows_to_insert)} rows for storage")
        return rows_to_insert

    def save_data(self, data):
        """
        Save the extracted data to local CSV storage

        Args:
            data: List of data dictionaries to save

        Returns:
            bool: Success or failure
        """
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)

            if df.empty:
                logger.warning("No data to save")
                return True

            # Clean data for CSV storage
            df = self.clean_dataframe_for_storage(df)

            # Load existing data if file exists and has content
            if (
                os.path.exists(self.master_file)
                and os.path.getsize(self.master_file) > 10
            ):
                existing_df = pd.read_csv(self.master_file, encoding="utf-8-sig")

                # Append new data
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                logger.info(f"Added {len(df)} rows to existing {len(existing_df)} rows")
            else:
                combined_df = df
                logger.info(f"Created new master file with {len(df)} rows")

            # Save to master file
            combined_df.to_csv(self.master_file, index=False, encoding="utf-8-sig")
            logger.info(f"Successfully saved data to {self.master_file}")

            # Update threads file with new thread IDs
            self._update_threads_file(df)

            return True
        except Exception as e:
            logger.error(f"Error saving data to local storage: {e}")
            return False

    def _update_threads_file(self, df):
        """
        Update the threads file with new thread IDs from the data

        Args:
            df: DataFrame containing new data with thread_id column
        """
        try:
            if "thread_id" not in df.columns:
                logger.warning("No thread_id column in data, skipping threads update")
                return

            # Extract unique thread IDs and create DataFrame with current timestamp
            thread_ids = df["thread_id"].unique()
            threads_df = pd.DataFrame(
                {
                    "thread_id": thread_ids,
                    "processed_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

            # Load existing threads if file exists and has content
            if (
                os.path.exists(self.threads_file)
                and os.path.getsize(self.threads_file) > 10
            ):
                existing_threads = pd.read_csv(self.threads_file, encoding="utf-8-sig")

                # Append new threads
                combined_threads = pd.concat(
                    [existing_threads, threads_df], ignore_index=True
                )

                # Remove duplicates (keep the latest)
                combined_threads = combined_threads.sort_values(
                    "processed_date", ascending=False
                )
                combined_threads = combined_threads.drop_duplicates(
                    "thread_id", keep="first"
                )
            else:
                combined_threads = threads_df

            # Save to threads file
            combined_threads.to_csv(
                self.threads_file, index=False, encoding="utf-8-sig"
            )
            logger.info(f"Updated threads file with {len(thread_ids)} new thread IDs")
        except Exception as e:
            logger.error(f"Error updating threads file: {e}")

    def query_threads_within_day(self, days=7):
        """
        Get list of thread IDs processed within specified days
        Similar to the original BigQuery function

        Args:
            days: Number of days to look back

        Returns:
            list: List of thread IDs
        """
        try:
            # If threads file doesn't exist or is empty, return empty list
            if (
                not os.path.exists(self.threads_file)
                or os.path.getsize(self.threads_file) <= 10
            ):
                logger.info("No threads file found or empty file, returning empty list")
                return []

            # Load threads file
            threads_df = pd.read_csv(self.threads_file, encoding="utf-8-sig")

            if threads_df.empty:
                return []

            # Convert processed_date to datetime
            threads_df["processed_date"] = pd.to_datetime(
                threads_df["processed_date"], errors="coerce"
            )

            # Filter for threads processed within the specified days
            cutoff_date = datetime.now() - timedelta(days=days)
            recent_threads = threads_df[threads_df["processed_date"] >= cutoff_date]

            # Return list of thread IDs
            thread_ids = recent_threads["thread_id"].tolist()
            logger.info(
                f"Found {len(thread_ids)} threads processed in the last {days} days"
            )

            return thread_ids
        except Exception as e:
            logger.error(f"Error querying threads within day: {e}")
            return []

    def clean_dataframe_for_storage(self, df):
        """
        Clean dataframe for storage in CSV files

        Args:
            df: DataFrame to clean

        Returns:
            DataFrame: Cleaned DataFrame
        """
        # Columns that should be numeric
        numeric_columns = [
            "amount_before_tax",
            "tax_rate",
            "tax_amount",
            "total_amount",
        ]

        for col in numeric_columns:
            if col in df.columns:
                # Replace empty strings with NaN
                df[col] = df[col].replace("", np.nan)
                # Convert to float
                df[col] = df[col].astype(float)

        return df

    def query_transactions_by_date(
        self, start_date=None, end_date=None, entity_name=None
    ):
        """
        Query transaction data from local storage by date range and optional entity name
        Replacement for the BigQuery version

        Args:
            start_date: Start date in format 'YYYY-MM-DD'
            end_date: End date in format 'YYYY-MM-DD'
            entity_name: Optional entity name to filter by

        Returns:
            DataFrame: Filtered transaction data
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

            # Check if master file exists
            if (
                not os.path.exists(self.master_file)
                or os.path.getsize(self.master_file) <= 10
            ):
                logger.warning("Master file does not exist or is empty")
                return pd.DataFrame()

            # Load master file
            df = pd.read_csv(self.master_file, encoding="utf-8-sig")

            if df.empty:
                logger.warning("No data in master file")
                return df

            # Convert date to datetime
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

            # Filter by date range
            mask = (df["date"] >= start_date) & (df["date"] <= end_date)
            filtered_df = df[mask]

            # Filter by document type
            filtered_df = filtered_df[filtered_df["document_type"] == "INVOICE"]

            # Filter by entity name if provided
            if entity_name:
                filtered_df = filtered_df[filtered_df["entity_name"] == entity_name]

            logger.info(f"Query returned {len(filtered_df)} rows")
            return filtered_df

        except Exception as e:
            logger.error(f"Error querying transactions: {e}")
            return pd.DataFrame()

    def upload_dataframe(self, data, **kwargs):
        """
        Compatibility method to match BigQuery interface
        Simply calls save_data

        Args:
            data: DataFrame to save
            **kwargs: Additional arguments (ignored)

        Returns:
            bool: Success or failure
        """
        return self.save_data(data.to_dict("records"))

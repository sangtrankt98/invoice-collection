#!/usr/bin/env python3
"""
Invoice Collection Main Script
Automates the process of collecting invoice data from emails and uploading to BigQuery
"""
import os
import logging
from datetime import datetime
from utils.auth import GoogleAuthenticator
from utils.gmail_handler import GmailHandler
from utils.bigquery_uploader import BigQueryUploader
import config


# Set up logging
def setup_logger():
    """Configure the logger"""
    logger = logging.getLogger("invoice_collection")
    logger.setLevel(logging.INFO)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create file handler
    file_handler = logging.FileHandler("invoice_collection.log")
    file_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def main():
    """Main function to orchestrate the invoice collection process"""
    # Set up logger
    logger = setup_logger()

    # Authenticate with Google services
    logger.info("Authenticating with Google services...")
    authenticator = GoogleAuthenticator(config.SCOPES)
    credentials = authenticator.get_credentials()

    # Initialize handlers
    gmail_handler = GmailHandler(credentials)
    bigquery_uploader = BigQueryUploader(credentials)

    # Process emails
    logger.info("Fetching and processing emails...")
    email_data = gmail_handler.extract_email_content(
        query=config.EMAIL_QUERY, max_results=config.MAX_EMAILS
    )

    if not email_data:
        logger.warning("No email data to process.")
        return

    logger.info(f"Processed {len(email_data)} emails.")

    # Upload to BigQuery
    logger.info("Uploading to BigQuery...")
    errors = bigquery_uploader.upload_data(
        email_data, dataset_id=config.BQ_DATASET, table_id=config.BQ_TABLE
    )

    if not errors:
        logger.info("Process completed successfully.")
    else:
        logger.error(f"Process completed with errors: {errors}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Invoice Collection Main Script
Automates the process of collecting invoice data from emails and uploading to BigQuery
"""
import os
import sys
from datetime import datetime
from utils.auth import GoogleAuthenticator
from utils.gmail_handler import GmailHandler
from utils.bigquery_uploader import BigQueryUploader
from utils.logger_setup import setup_logger
from utils.openai import InvoiceExtractor
import config


def main():
    """Main function to orchestrate the invoice collection process"""
    # Set up logger
    logger = setup_logger()

    try:
        logger.info("Starting invoice collection process")

        # Authenticate with Google services
        logger.info("Authenticating with Google services...")
        authenticator = GoogleAuthenticator(config.SCOPES)
        credentials = authenticator.get_credentials()

        # Initialize handlers
        logger.info("Initializing service handlers...")
        gmail_handler = GmailHandler(credentials, config.OPENAI)
        bigquery_uploader = BigQueryUploader(credentials)
        # Process emails
        logger.info(f"Fetching emails with query: '{config.EMAIL_QUERY}'...")
        email_data = gmail_handler.extract_email_content(
            query=config.EMAIL_QUERY, max_results=config.MAX_EMAILS
        )
        if not email_data:
            logger.warning("No email data to process.")
            return

        logger.info(f"Processed {len(email_data)} emails.")

        # Upload to BigQuery
        logger.info(
            f"Uploading data to BigQuery table {config.BQ_DATASET}.{config.BQ_TABLE}..."
        )
        errors = bigquery_uploader.upload_data(
            email_data, dataset_id=config.BQ_DATASET, table_id=config.BQ_TABLE
        )

        if not errors:
            logger.info("Process completed successfully.")
        else:
            logger.error(f"Process completed with errors: {errors}")

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

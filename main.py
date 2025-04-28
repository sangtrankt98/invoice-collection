#!/usr/bin/env python3
"""
Invoice Collection Main Script
Automates the process of collecting invoice data from emails and uploading to BigQuery
"""
import sys
import pandas as pd
from datetime import datetime, timedelta
from utils.auth import GoogleAuthenticator
from utils.gmail_handler import GmailHandler
from utils.drive_handler import DriveHandler
from utils.local_handler import LocalHandler
from utils.bigquery_handler import BigQueryHandler
from utils.output_handler import *
from utils.logger_setup import setup_logger
import config
import argparse
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)


def run_invoice_collection(drive_link):
    """
    Main function to orchestrate the invoice collection process
    Returns a tuple: (success_flag, message)
    """
    logger = setup_logger()

    try:
        logger.info("Starting invoice collection process")
        logger.info("Authenticating with Google services...")
        authenticator = GoogleAuthenticator(config.SCOPES)
        credentials = authenticator.get_credentials()

        logger.info("Initializing service handlers...")
        gmail_handler = GmailHandler(
            credentials, config.OPENAI, DriveHandler(credentials)
        )
        bigquery_handler = BigQueryHandler(credentials)
        drive_handler = DriveHandler(credentials)  # Drive handler

        # Get current time and subtract 30 mins
        now = datetime.utcnow()
        delta = timedelta(minutes=30)
        time_threshold = now - delta
        # Convert to Unix timestamp
        unix_time = int(time_threshold.timestamp())
        EMAIL_QUERY = f"newer_than:1d"
        logger.info(f"Fetching emails with query: '{EMAIL_QUERY}'...")

        email_data = gmail_handler.extract_email_content(
            query=EMAIL_QUERY, max_results=config.MAX_EMAILS
        )
        if not email_data:
            logger.warning("No email data to process.")
            return False, "No email data found for the specified date range."

        logger.info(f"Successfully processed {len(email_data)} emails.")

        logger.info("Extracting data from emails...")
        df = pd.DataFrame(bigquery_handler.extract_data(email_data))

        if df.empty:
            logger.warning("No data extracted from emails.")
            return False, "No data could be extracted from the emails."

        logger.info("Processing attachment details...")
        df_exploded = df.explode("attachment_details")
        attachment_df = pd.json_normalize(df_exploded["attachment_details"])
        final_df = pd.concat(
            [
                df_exploded.drop(columns=["attachment_details"]).reset_index(drop=True),
                attachment_df.reset_index(drop=True),
            ],
            axis=1,
        )

        # Standardize entity names and determine transaction direction
        logger.info("Standardizing entities and determining transaction direction...")
        final_df = standardize_dataframe(final_df)

        # Format document numbers
        final_df["document_number"] = "'" + final_df["document_number"].astype(str)

        # Save the processed data
        # logger.info("Saving data to CSV file...")
        # final_df.to_csv("flattened_attachments.csv", index=False, encoding="utf-8-sig")
        # logger.info(f"CSV saved successfully with {len(final_df)} rows.")

        # Upload data to bigquery
        final_df = bigquery_handler.clean_dataframe_for_bigquery(final_df)
        bigquery_handler.upload_dataframe(
            data=final_df,
            project_id="immortal-0804",
            dataset_id="finance_project",
            table_id="invoice_summarize",
            if_exists="append",  # will append to table if it exists
        )

        # Upload PDF files to Google Drive
        logger.info(f"Organizing and uploading PDFs to Drive folder: {drive_link}")
        upload_results = drive_handler.organize_and_upload_pdfs(
            drive_link, final_df, local_pdf_dir="downloads"
        )
        # Check for drive errors
        if "error" in upload_results:
            return False, upload_results["error"]

        logger.info(
            f"Upload summary: {upload_results['successful_uploads']} successful, "
            f"{upload_results['failed_uploads']} failed"
        )

        if upload_results["failed_uploads"] > 0:
            logger.warning("Some files failed to upload. Check the log for details.")

        # Initialize with a base directory
        local_handler = LocalHandler(r"C:\Users\NJV\source_code\invoice-collection")

        # Organize PDFs from email data
        result = local_handler.organize_and_copy_pdfs(
            "company_files",  # Target directory (will be created if it doesn't exist)
            final_df,  # DataFrame with company and file information
            "downloads",  # Source directory containing PDFs
        )
        # Check for drive errors
        if "error" in result:
            return False, result["error"]
        return (
            True,
            f"Process completed successfully. Uploaded {upload_results['successful_uploads']} files. Copy summary: {result['successful_copies']} successful",
        )
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        return False, "Process was interrupted by user."
    except Exception as e:
        error_msg = f"An unexpected error occurred: {str(e)}"
        logger.exception(error_msg)
        return False, error_msg


def main():
    """Main entry point with command line argument parsing"""
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Invoice Collection Tool")

    # Add optional drive-link argument
    parser.add_argument(
        "--drive-link",
        help="Google Drive folder link where files will be organized",
        default=config.DRIVE,
    )

    args = parser.parse_args()

    # Set up logging
    logger = setup_logger()
    logger.info("Application started")

    # Run the invoice collection process
    success, message = run_invoice_collection(drive_link=args.drive_link)

    if success:
        logger.info(message)
        print(f"\nSUCCESS: {message}")
        return 0
    else:
        logger.error(f"Process failed: {message}")
        print(f"\nERROR: {message}")
        return 1


# Main execution
if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Invoice Collection Main Script
Automates the process of collecting invoice data from emails and uploading to BigQuery
Optimized for PyInstaller packaging and automation
"""
import sys
import pandas as pd
from datetime import datetime, timedelta
import os
import warnings
from utils.auth import GoogleAuthenticator
from utils.gmail_handler import GmailHandler
from utils.drive_handler import DriveHandler
from utils.local_handler import LocalHandler
from utils.bigquery_handler import BigQueryHandler
from utils.output_handler import standardize_dataframe
from utils.logger_setup import setup_logger
import config

warnings.simplefilter(action="ignore", category=FutureWarning)

logger = setup_logger()


def convert_time_to_query(time_filter):
    """
    Convert time filter to a Gmail-compatible query format
    """
    # Parse the time filter
    unit = time_filter[-1]  # Get the last character (unit)
    try:
        value = int(time_filter[:-1])  # Get the numeric value
    except ValueError:
        # Default to 1d if parsing fails
        logger.warning(f"Invalid time filter format: {time_filter}, defaulting to 1d")
        return "newer_than:1d"

    # For days, we can use the standard newer_than:Nd format
    if unit == "d":
        return f"newer_than:{value}d"

    # For hours and minutes, use a date-based query
    if unit == "h":
        now = datetime.now()

        if value < 24:
            # For less than 24 hours, get today's emails (and yesterday's if needed)
            if now.hour < value:
                # Need to include yesterday's emails too
                yesterday = (now - timedelta(days=1)).strftime("%Y/%m/%d")
                logger.info(
                    f"Hour filter spans to previous day, using after:{yesterday}"
                )
                return f"after:{yesterday}"
            else:
                # Can just use today's emails
                today = now.strftime("%Y/%m/%d")
                logger.info(f"Hour filter within today, using after:{today}")
                return f"after:{today}"
        else:
            # For 24+ hours, convert to days (rounded up)
            days = (value + 23) // 24  # Round up
            logger.info(
                f"Converting {value} hours to {days} days, using newer_than:{days}d"
            )
            return f"newer_than:{days}d"

    elif unit == "m":
        # For minutes, we'll get today's emails and filter them afterward
        today = datetime.now().strftime("%Y/%m/%d")
        logger.info(f"Minute filter, using broader query after:{today}")
        return f"after:{today}"

    # Default to 1 day if unit is not recognized
    logger.warning(f"Unrecognized time filter unit: {unit}, defaulting to 1d")
    return "newer_than:1d"


def run_invoice_collection(drive_link, time_filter="1d"):
    """
    Main function to orchestrate the invoice collection process

    Args:
        drive_link: Google Drive folder link for organizing files
        time_filter: Time period for email filtering (default: "1d")

    Returns:
        tuple: (success_flag, message)
    """
    try:

        def log_status(message):
            logger.info(message)

        log_status("Starting invoice collection process...")
        log_status(f"Using time filter: {time_filter}")

        log_status("Authenticating with Google services...")
        authenticator = GoogleAuthenticator(config.SCOPES)
        credentials = authenticator.get_credentials()

        log_status("Initializing service handlers...")
        drive_handler = DriveHandler(credentials)
        gmail_handler = GmailHandler(credentials, config.OPENAI, drive_handler)
        bigquery_handler = BigQueryHandler(credentials)

        # Convert time filter to Gmail-compatible query
        EMAIL_QUERY = convert_time_to_query(time_filter)
        log_status(f"Fetching emails with query: '{EMAIL_QUERY}'...")

        # Collect processed threads within a day
        threads = bigquery_handler.query_threads_within_day()
        # Use the updated extract_email_content method with time_filter for post-processing
        email_data = gmail_handler.extract_email_content(
            query=EMAIL_QUERY,
            max_results=config.MAX_EMAILS,
            time_filter=time_filter,  # Pass the time filter for post-processing
            processed_thread_ids=threads,
        )
        if not email_data:
            logger.warning("No email data to process.")
            return False, "No email data found for the specified time period."

        log_status(f"Successfully processed {len(email_data)} emails.")

        log_status("Extracting data from emails...")
        df = pd.DataFrame(bigquery_handler.extract_data(email_data))

        if df.empty:
            logger.warning("No data extracted from emails.")
            return False, "No data could be extracted from the emails."

        log_status("Processing attachment details...")
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
        log_status("Standardizing entities and determining transaction direction...")
        final_df = standardize_dataframe(final_df)

        # Format document numbers
        final_df["document_number"] = "'" + final_df["document_number"].astype(str)

        # Upload data to bigquery
        log_status("Uploading data to BigQuery...")
        final_df = bigquery_handler.clean_dataframe_for_bigquery(final_df)
        bigquery_handler.upload_dataframe(
            data=final_df,
            project_id="immortal-0804",
            dataset_id="finance_project",
            table_id="invoice_summarize",
            if_exists="append",  # will append to table if it exists
        )

        # # Upload PDF files to Google Drive
        # log_status(f"Organizing and uploading PDFs to Drive folder...")
        # upload_results = drive_handler.organize_and_upload_pdfs(
        #     drive_link, final_df, local_pdf_dir="downloads"
        # )
        # # Check for drive errors
        # if "error" in upload_results:
        #     error_msg = upload_results["error"]
        #     logger.error(error_msg)
        #     return False, error_msg

        # log_status(
        #     f"Upload summary: {upload_results['successful_uploads']} successful, "
        #     f"{upload_results['failed_uploads']} failed"
        # )

        # if upload_results["failed_uploads"] > 0:
        #     logger.warning("Some files failed to upload. Check the log for details.")

        # Get base directory that works in both development and PyInstaller context
        base_dir = os.path.abspath(
            os.path.dirname(sys.executable)
            if getattr(sys, "frozen", False)
            else os.path.dirname(__file__)
        )

        # Initialize with base directory
        local_handler = LocalHandler(base_dir)

        # Organize PDFs from email data
        log_status("Organizing PDFs locally...")
        result = local_handler.organize_and_copy_pdfs(
            "company_files",  # Target directory (will be created if it doesn't exist)
            final_df,  # DataFrame with company and file information
            "downloads",  # Source directory containing PDFs
        )
        # Check for local copy errors
        if "error" in result:
            error_msg = result["error"]
            logger.error(error_msg)
            return False, error_msg

        success_message = f"Process completed successfully. Copy summary: {result['successful_copies']} successful."
        log_status(success_message)

        return True, success_message
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        return False, "Process was interrupted by user."
    except Exception as e:
        error_msg = f"An unexpected error occurred: {str(e)}"
        logger.exception(error_msg)
        return False, error_msg


def main():
    """Main entry point for the application"""
    # Set up logging
    logger = setup_logger()
    logger.info("Application started")

    # Load drive link from config
    drive_link = config.DRIVE
    # Set default time filter to 1 day
    time_filter = "11h"
    logger.info(f"Starting process with time filter: {time_filter}")
    success, message = run_invoice_collection(
        drive_link=drive_link, time_filter=time_filter
    )

    if success:
        logger.info("Process completed successfully")
        return 0
    else:
        logger.error(f"Process failed: {message}")
        return 1


# Main execution
if __name__ == "__main__":
    sys.exit(main())

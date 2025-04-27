#!/usr/bin/env python3
"""
Main Report Generator Script
Queries transaction data from BigQuery and generates financial reports by company
Supports both individual company reporting and mass generation
"""
import sys
import os
import argparse
from datetime import datetime, timedelta
import pandas as pd
import logging
from utils.auth import GoogleAuthenticator
from utils.bigquery_handler import BigQueryHandler
from utils.transaction_reports import TransactionReportGenerator
import config


# Set up logger
def setup_logger():
    """Setup application logger"""
    logger = logging.getLogger("report_generator")
    logger.setLevel(logging.INFO)

    # Create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)

    # Add handler to logger if it doesn't already have one
    if not logger.handlers:
        logger.addHandler(ch)

        # Create file handler
        fh = logging.FileHandler("report_generation.log")
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def generate_entity_report(
    entity_name, transaction_data, output_folder, template_file, date_range_str
):
    """
    Generate reports for a specific entity

    Args:
        entity_name (str): Name of the entity
        transaction_data (DataFrame): Filtered transaction data for this entity
        output_folder (str): Base output folder
        template_file (str): Template file path
        date_range_str (str): Date range string for folder name

    Returns:
        dict: Results of report generation
    """
    logger = setup_logger()

    try:
        # Skip empty entity names
        if not entity_name or pd.isna(entity_name) or str(entity_name).strip() == "":
            return {
                "success": False,
                "message": "Empty entity name",
                "reports_generated": 0,
            }

        # Create entity folder
        entity_folder = os.path.join(
            output_folder, sanitize_filename(entity_name), date_range_str
        )
        os.makedirs(entity_folder, exist_ok=True)
        logger.info(
            f"Generating reports for entity: {entity_name} in folder: {entity_folder}"
        )

        # Save entity-specific data
        entity_csv = os.path.join(entity_folder, "transactions.csv")
        transaction_data.to_csv(entity_csv, index=False, encoding="utf-8-sig")

        # Generate reports for this entity
        generator = TransactionReportGenerator(
            input_file=entity_csv, template_file=template_file
        )

        result = generator.generate_all_reports(base_folder=entity_folder)

        if result:
            logger.info(f"Successfully generated reports for {entity_name}")
            return {
                "success": True,
                "message": f"Successfully generated reports for {entity_name}",
                "reports_generated": 2,  # Incoming and outgoing
            }
        else:
            logger.warning(f"Failed to generate reports for {entity_name}")
            return {
                "success": False,
                "message": f"Failed to generate reports for {entity_name}",
                "reports_generated": 0,
            }
    except Exception as e:
        logger.error(f"Error generating reports for {entity_name}: {str(e)}")
        return {"success": False, "message": f"Error: {str(e)}", "reports_generated": 0}


def generate_reports(
    start_date=None,
    end_date=None,
    entity_name=None,
    output_folder="company_reports",
    template_file=None,
    mass_generation=False,
):
    """
    Generate financial reports for specified date range and entity

    Args:
        start_date (str): Start date in format YYYY-MM-DD
        end_date (str): End date in format YYYY-MM-DD
        entity_name (str, optional): Specific entity to generate reports for
        output_folder (str): Directory to save reports
        template_file (str, optional): Excel template file path
        mass_generation (bool): Whether to generate reports for all entities

    Returns:
        dict: Results with success/failure information
    """
    logger = setup_logger()

    try:
        logger.info("Starting report generation process")

        # Validate date formats
        if start_date:
            try:
                datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                return {
                    "success": False,
                    "message": "Invalid start_date format. Use YYYY-MM-DD",
                }

        if end_date:
            try:
                datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                return {
                    "success": False,
                    "message": "Invalid end_date format. Use YYYY-MM-DD",
                }

        # If no dates specified, use last 30 days
        if not start_date and not end_date:
            end_date_obj = datetime.now()
            start_date_obj = end_date_obj - timedelta(days=30)
            start_date = start_date_obj.strftime("%Y-%m-%d")
            end_date = end_date_obj.strftime("%Y-%m-%d")
            logger.info(
                f"No date range specified. Using default: {start_date} to {end_date}"
            )

        # Authenticate with Google
        logger.info("Authenticating with Google services...")
        authenticator = GoogleAuthenticator(config.SCOPES)
        credentials = authenticator.get_credentials()

        # Initialize BigQuery handler
        bigquery_handler = BigQueryHandler(credentials)

        # Force mass generation if explicitly requested, or if no entity specified
        if mass_generation:
            logger.info("Mass generation mode: will generate reports for all entities")
            entity_name = None  # Clear entity name to process all

        # Query data from BigQuery
        logger.info(
            f"Querying BigQuery for transactions from {start_date} to {end_date}"
        )
        if entity_name and not mass_generation:
            logger.info(f"Filtering for entity: {entity_name}")

        transaction_data = bigquery_handler.query_transactions_by_date(
            start_date=start_date, end_date=end_date, entity_name=entity_name
        )

        if transaction_data.empty:
            logger.warning("No transaction data found for the specified criteria")
            return {
                "success": False,
                "message": "No transaction data found for the specified date range and entity",
                "entities_processed": 0,
                "reports_generated": 0,
            }

        logger.info(f"Retrieved {len(transaction_data)} transaction records")

        # Create date range folder string
        date_range = f"{start_date}_to_{end_date}"

        # Create reports directory if it doesn't exist
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            logger.info(f"Created output directory: {output_folder}")

        # Track results
        entities_processed = 0
        reports_generated = 0
        entity_results = []

        # Process by entity
        if entity_name and not mass_generation:
            # Process specific entity
            entity_result = generate_entity_report(
                entity_name=entity_name,
                transaction_data=transaction_data,
                output_folder=output_folder,
                template_file=template_file,
                date_range_str=date_range,
            )

            entity_results.append(entity_result)

            if entity_result["success"]:
                entities_processed = 1
                reports_generated = entity_result["reports_generated"]
        else:
            # Process all entities (mass generation)
            unique_entities = transaction_data["entity_name"].dropna().unique()
            logger.info(f"Found {len(unique_entities)} unique entities in data")

            for entity in unique_entities:
                # Filter to just this entity
                entity_df = transaction_data[transaction_data["entity_name"] == entity]

                if not entity_df.empty:
                    entity_result = generate_entity_report(
                        entity_name=entity,
                        transaction_data=entity_df,
                        output_folder=output_folder,
                        template_file=template_file,
                        date_range_str=date_range,
                    )

                    entity_results.append(entity_result)

                    if entity_result["success"]:
                        entities_processed += 1
                        reports_generated += entity_result["reports_generated"]

        # Compile results
        success = entities_processed > 0
        message = f"Processed {entities_processed} entities, generated {reports_generated} reports"

        if entity_results:
            # List successfully processed entities
            successful_entities = [
                result.get("entity_name", "Unknown")
                for result in entity_results
                if result.get("success")
            ]
            failed_entities = [
                result.get("entity_name", "Unknown")
                for result in entity_results
                if not result.get("success")
            ]

            if successful_entities:
                message += (
                    f"\nSuccessful entities: {', '.join(successful_entities[:5])}"
                )
                if len(successful_entities) > 5:
                    message += f" and {len(successful_entities) - 5} more"

            if failed_entities:
                message += f"\nFailed entities: {', '.join(failed_entities[:5])}"
                if len(failed_entities) > 5:
                    message += f" and {len(failed_entities) - 5} more"

        return {
            "success": success,
            "message": message,
            "entities_processed": entities_processed,
            "reports_generated": reports_generated,
            "date_range": date_range,
            "entity_results": entity_results,
        }

    except Exception as e:
        logger.exception(f"Error generating reports: {str(e)}")

        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "entities_processed": 0,
            "reports_generated": 0,
        }


def sanitize_filename(filename):
    """
    Sanitize a string to be used as a directory or filename

    Args:
        filename (str): Original filename

    Returns:
        str: Sanitized filename
    """
    if not filename:
        return "unknown"

    # Replace problematic characters
    invalid_chars = r'<>:"/\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, "_")

    # Remove leading/trailing spaces and dots
    filename = filename.strip(". ")

    # Limit length
    if len(filename) > 100:
        filename = filename[:100]

    return filename if filename else "unknown"


def main():
    """Main entry point with command line argument parsing"""
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Financial Report Generator")

    parser.add_argument(
        "--start-date",
        help="Start date for report period (YYYY-MM-DD). Defaults to 30 days ago.",
    )

    parser.add_argument(
        "--end-date", help="End date for report period (YYYY-MM-DD). Defaults to today."
    )

    parser.add_argument("--entity", help="Optional entity name to filter by")

    parser.add_argument(
        "--output-folder",
        default="company_reports",
        help="Folder to save reports. Defaults to 'company_reports'",
    )

    parser.add_argument(
        "--template", help="Optional Excel template file to use for reports"
    )

    parser.add_argument(
        "--mass-generation",
        action="store_true",
        help="Generate reports for all entities within the date range",
    )

    args = parser.parse_args()

    # Call the report generation function
    result = generate_reports(
        start_date=args.start_date,
        end_date=args.end_date,
        entity_name=args.entity,
        output_folder=args.output_folder,
        template_file=args.template,
        mass_generation=args.mass_generation,
    )

    # Print result summary
    if result["success"]:
        print(f"\nSUCCESS: {result['message']}")
        print(f"Reports saved in: {args.output_folder}")
        return 0
    else:
        print(f"\nERROR: {result['message']}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

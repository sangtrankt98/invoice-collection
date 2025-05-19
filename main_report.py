#!/usr/bin/env python3
"""
Report Generator with GUI
Queries transaction data from BigQuery and generates financial reports by company
Features a simple GUI for easier operation
"""
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import threading
import time
from utils.auth import GoogleAuthenticator
from archived.bigquery_handler import BigQueryHandler
from utils.transaction_reports import TransactionReportGenerator
from utils.logger_setup import setup_logger
import config
import tkinter as tk
from tkinter import ttk, scrolledtext, simpledialog, messagebox

logger = setup_logger()


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
                "entity_name": entity_name,
            }
        else:
            logger.warning(f"Failed to generate reports for {entity_name}")
            return {
                "success": False,
                "message": f"Failed to generate reports for {entity_name}",
                "reports_generated": 0,
                "entity_name": entity_name,
            }
    except Exception as e:
        logger.error(f"Error generating reports for {entity_name}: {str(e)}")
        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "reports_generated": 0,
            "entity_name": entity_name,
        }


def generate_reports(
    start_date=None,
    end_date=None,
    entity_name=None,
    output_folder="company_reports",
    template_file=None,
    mass_generation=False,
    status_callback=None,
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
        status_callback (function): Callback function to update status in GUI

    Returns:
        dict: Results with success/failure information
    """
    logger = setup_logger()

    try:

        def update_status(message):
            if status_callback:
                status_callback(message)
            logger.info(message)

        update_status("Starting report generation process")

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
            update_status(
                f"No date range specified. Using default: {start_date} to {end_date}"
            )

        # Authenticate with Google
        update_status("Authenticating with Google services...")
        authenticator = GoogleAuthenticator(config.SCOPES)
        credentials = authenticator.get_credentials()

        # Initialize BigQuery handler
        bigquery_handler = BigQueryHandler(credentials)

        # Force mass generation if explicitly requested, or if no entity specified
        if mass_generation:
            update_status(
                "Mass generation mode: will generate reports for all entities"
            )
            entity_name = None  # Clear entity name to process all

        # Query data from BigQuery
        update_status(
            f"Querying BigQuery for transactions from {start_date} to {end_date}"
        )
        if entity_name and not mass_generation:
            update_status(f"Filtering for entity: {entity_name}")

        transaction_data = bigquery_handler.query_transactions_by_date(
            start_date=start_date, end_date=end_date, entity_name=entity_name
        )

        if transaction_data.empty:
            update_status("No transaction data found for the specified criteria")
            return {
                "success": False,
                "message": "No transaction data found for the specified date range and entity",
                "entities_processed": 0,
                "reports_generated": 0,
            }

        update_status(f"Retrieved {len(transaction_data)} transaction records")

        # Create date range folder string
        date_range = f"{start_date}_to_{end_date}"

        # Create reports directory if it doesn't exist
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            update_status(f"Created output directory: {output_folder}")

        # Track results
        entities_processed = 0
        reports_generated = 0
        entity_results = []

        # Process by entity
        if entity_name and not mass_generation:
            # Process specific entity
            update_status(f"Generating report for entity: {entity_name}")
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
            update_status(f"Found {len(unique_entities)} unique entities in data")

            for i, entity in enumerate(unique_entities):
                update_status(
                    f"Processing entity {i+1}/{len(unique_entities)}: {entity}"
                )
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

        update_status(f"Report generation completed: {message}")
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

        if status_callback:
            status_callback(f"Error: {str(e)}")

        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "entities_processed": 0,
            "reports_generated": 0,
        }


def create_gui():
    """Create and return the GUI application"""
    root = tk.Tk()
    root.title("Financial Report Generator")
    root.geometry("600x600")
    root.minsize(600, 550)
    root.resizable(True, True)

    # Variables
    status_var = tk.StringVar(value="Ready")
    entity_var = tk.StringVar(value="")
    start_date_var = tk.StringVar(value="")  # Default empty
    end_date_var = tk.StringVar(value="")  # Default empty
    output_folder_var = tk.StringVar(value="company_reports")
    template_file_var = tk.StringVar(value="")
    mass_generation_var = tk.BooleanVar(value=False)
    running = False

    # Center the window
    def center_window():
        root.update_idletasks()
        width = root.winfo_width()
        height = root.winfo_height()
        x = (root.winfo_screenwidth() // 2) - (width // 2)
        y = (root.winfo_screenheight() // 2) - (height // 2)
        root.geometry("{}x{}+{}+{}".format(width, height, x, y))

    # Main frame with padding
    main_frame = ttk.Frame(root, padding="20 20 20 20")
    main_frame.pack(fill=tk.BOTH, expand=True)

    # Title
    title_label = ttk.Label(
        main_frame, text="Financial Report Generator", font=("Arial", 16, "bold")
    )
    title_label.pack(pady=(0, 15))

    # Input frame
    input_frame = ttk.LabelFrame(
        main_frame, text="Input Parameters", padding="10 10 10 10"
    )
    input_frame.pack(fill=tk.X, pady=(0, 10))

    # Start date
    start_date_frame = ttk.Frame(input_frame)
    start_date_frame.pack(fill=tk.X, pady=5)

    start_date_label = ttk.Label(
        start_date_frame, text="Start Date (YYYY-MM-DD):", width=25
    )
    start_date_label.pack(side=tk.LEFT, padx=(0, 5))

    start_date_entry = ttk.Entry(
        start_date_frame, textvariable=start_date_var, width=40
    )
    start_date_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

    # End date
    end_date_frame = ttk.Frame(input_frame)
    end_date_frame.pack(fill=tk.X, pady=5)

    end_date_label = ttk.Label(end_date_frame, text="End Date (YYYY-MM-DD):", width=25)
    end_date_label.pack(side=tk.LEFT, padx=(0, 5))

    end_date_entry = ttk.Entry(end_date_frame, textvariable=end_date_var, width=40)
    end_date_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

    # Entity name
    entity_frame = ttk.Frame(input_frame)
    entity_frame.pack(fill=tk.X, pady=5)

    entity_label = ttk.Label(entity_frame, text="Entity Name (optional):", width=25)
    entity_label.pack(side=tk.LEFT, padx=(0, 5))

    entity_entry = ttk.Entry(entity_frame, textvariable=entity_var, width=40)
    entity_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

    # Output folder
    output_folder_frame = ttk.Frame(input_frame)
    output_folder_frame.pack(fill=tk.X, pady=5)

    output_folder_label = ttk.Label(
        output_folder_frame, text="Output Folder:", width=25
    )
    output_folder_label.pack(side=tk.LEFT, padx=(0, 5))

    output_folder_entry = ttk.Entry(
        output_folder_frame, textvariable=output_folder_var, width=40
    )
    output_folder_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

    # Template file
    template_file_frame = ttk.Frame(input_frame)
    template_file_frame.pack(fill=tk.X, pady=5)

    template_file_label = ttk.Label(
        template_file_frame, text="Template File (optional):", width=25
    )
    template_file_label.pack(side=tk.LEFT, padx=(0, 5))

    template_file_entry = ttk.Entry(
        template_file_frame, textvariable=template_file_var, width=40
    )
    template_file_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

    # Mass generation checkbox
    mass_gen_frame = ttk.Frame(input_frame)
    mass_gen_frame.pack(fill=tk.X, pady=5)

    mass_gen_check = ttk.Checkbutton(
        mass_gen_frame,
        text="Generate reports for all entities",
        variable=mass_generation_var,
    )
    mass_gen_check.pack(pady=5, padx=5, anchor=tk.W)

    # Help frame
    help_frame = ttk.LabelFrame(main_frame, text="Help", padding="10 10 10 10")
    help_frame.pack(fill=tk.X, pady=(0, 10))

    help_text = """- Leave date fields empty to use the last 30 days
- Enter an entity name to generate reports for a specific company
- Check 'Generate reports for all entities' to process all companies
- Reports will be saved in the specified output folder"""

    help_label = ttk.Label(help_frame, text=help_text, justify=tk.LEFT)
    help_label.pack(fill=tk.X)

    # Status and log area
    log_frame = ttk.LabelFrame(main_frame, text="Status", padding="10 10 10 10")
    log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

    log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, height=8)
    log_text.pack(fill=tk.BOTH, expand=True)
    log_text.config(state=tk.DISABLED)

    # Bottom buttons
    button_frame = ttk.Frame(main_frame)
    button_frame.pack(fill=tk.X, pady=(0, 5))

    status_label = ttk.Label(button_frame, textvariable=status_var, foreground="blue")
    status_label.pack(side=tk.LEFT)

    # Progress bar
    progress = ttk.Progressbar(main_frame, mode="indeterminate")
    progress.pack(fill=tk.X, pady=(0, 10))

    # Function to update log
    def update_log(message):
        log_text.config(state=tk.NORMAL)
        log_text.insert(tk.END, f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
        log_text.see(tk.END)
        log_text.config(state=tk.DISABLED)
        status_var.set(message)
        root.update_idletasks()

    # Function to handle process completion
    def process_complete(success, message):
        nonlocal running
        progress.stop()
        running = False
        run_button.config(state=tk.NORMAL)

        if success:
            status_var.set("Process completed successfully")
            messagebox.showinfo("Success", message)
        else:
            status_var.set("Process failed")
            messagebox.showerror("Error", message)

        update_log(message)

    # Function to run the process
    def run_process():
        nonlocal running
        if running:
            messagebox.showinfo("Already Running", "Process is already running.")
            return

        # Get input values
        start_date = start_date_var.get().strip()
        end_date = end_date_var.get().strip()
        entity_name = entity_var.get().strip()
        output_folder = output_folder_var.get().strip() or "company_reports"
        template_file = template_file_var.get().strip() or None
        mass_generation = mass_generation_var.get()

        # Update UI
        running = True
        run_button.config(state=tk.DISABLED)
        update_log(f"Starting process...")
        progress.start()

        # Run in a separate thread to keep UI responsive
        def threaded_run():
            result = generate_reports(
                start_date=start_date if start_date else None,
                end_date=end_date if end_date else None,
                entity_name=entity_name if entity_name else None,
                output_folder=output_folder,
                template_file=template_file,
                mass_generation=mass_generation,
                status_callback=update_log,
            )

            # Update UI with result
            root.after(
                0, lambda: process_complete(result["success"], result["message"])
            )

        threading.Thread(target=threaded_run, daemon=True).start()

    # Function to handle window close
    def on_close():
        if running:
            if messagebox.askyesno(
                "Confirm Exit", "Process is running. Are you sure you want to exit?"
            ):
                root.destroy()
        else:
            root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)

    # Create buttons
    buttons_container = ttk.Frame(button_frame, height=40)
    buttons_container.pack(side=tk.RIGHT)

    run_button = ttk.Button(
        buttons_container, text="Generate Reports", command=run_process
    )
    run_button.pack(side=tk.RIGHT, padx=5)

    exit_button = ttk.Button(buttons_container, text="Exit", command=on_close)
    exit_button.pack(side=tk.RIGHT)

    # Save settings button
    def save_settings():
        try:
            settings_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "report_settings.txt"
            )
            with open(settings_path, "w") as f:
                f.write(f"START_DATE={start_date_var.get()}\n")
                f.write(f"END_DATE={end_date_var.get()}\n")
                f.write(f"ENTITY_NAME={entity_var.get()}\n")
                f.write(f"OUTPUT_FOLDER={output_folder_var.get()}\n")
                f.write(f"TEMPLATE_FILE={template_file_var.get()}\n")
                f.write(f"MASS_GENERATION={mass_generation_var.get()}\n")
            update_log("Settings saved successfully")
            messagebox.showinfo(
                "Settings Saved", "Your settings have been saved successfully."
            )
        except Exception as e:
            update_log(f"Error saving settings: {str(e)}")
            messagebox.showerror("Error", f"Failed to save settings: {str(e)}")

    save_button = ttk.Button(
        buttons_container, text="Save Settings", command=save_settings
    )
    save_button.pack(side=tk.RIGHT, padx=5)

    # Try to load saved settings
    try:
        settings_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "report_settings.txt"
        )
        if os.path.exists(settings_path):
            with open(settings_path, "r") as f:
                for line in f:
                    if "=" in line:
                        key, value = line.strip().split("=", 1)
                        if key == "START_DATE":
                            start_date_var.set(value)
                        elif key == "END_DATE":
                            end_date_var.set(value)
                        elif key == "ENTITY_NAME":
                            entity_var.set(value)
                        elif key == "OUTPUT_FOLDER" and value:
                            output_folder_var.set(value)
                        elif key == "TEMPLATE_FILE":
                            template_file_var.set(value)
                        elif key == "MASS_GENERATION":
                            mass_generation_var.set(value.lower() == "true")
            update_log("Loaded saved settings")
    except Exception as e:
        logger.warning(f"Error loading settings: {str(e)}")

    # Center the window after all widgets are added
    center_window()

    return root


def main():
    """Main entry point without command line arguments"""
    # Set up logging
    logger = setup_logger()
    logger.info("Report Generator application started")

    # Start in GUI mode
    logger.info("Starting in GUI mode")
    try:
        root = create_gui()
        if not root:
            logger.error("Failed to create GUI. Exiting.")
            return 1

        root.mainloop()
        return 0
    except Exception as e:
        logger.exception(f"Error in GUI: {str(e)}")
        return 1


# Main execution
if __name__ == "__main__":
    sys.exit(main())

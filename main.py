#!/usr/bin/env python3
"""
Invoice Collection Main Script
Automates the process of collecting invoice data from emails and uploading to BigQuery
Optimized for PyInstaller packaging and Task Scheduler automation
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
import os
import threading
import time

# For GUI option when running as executable
try:
    import tkinter as tk
    from tkinter import ttk, scrolledtext, simpledialog, messagebox

    HAS_TK = True
except ImportError:
    HAS_TK = False

warnings.simplefilter(action="ignore", category=FutureWarning)

logger = setup_logger()


def convert_time_to_query(time_filter):
    """
    Convert time filter from the UI to a Gmail-compatible query format
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


def run_invoice_collection(
    drive_link, time_filter, status_callback=None, exit_on_complete=False
):
    """
    Main function to orchestrate the invoice collection process

    Args:
        drive_link: Google Drive folder link for organizing files
        time_filter: Time period for email filtering (e.g., "1d", "2h")
        status_callback: Optional callback function to update status in GUI
        exit_on_complete: Whether to exit the program after completion (for scheduled tasks)

    Returns:
        tuple: (success_flag, message)
    """
    logger = setup_logger()

    try:

        def update_status(message):
            if status_callback:
                status_callback(message)
            logger.info(message)

        update_status("Starting invoice collection process...")
        update_status(f"Using time filter: {time_filter}")

        update_status("Authenticating with Google services...")
        authenticator = GoogleAuthenticator(config.SCOPES)
        credentials = authenticator.get_credentials()

        update_status("Initializing service handlers...")
        gmail_handler = GmailHandler(
            credentials, config.OPENAI, DriveHandler(credentials)
        )
        bigquery_handler = BigQueryHandler(credentials)
        drive_handler = DriveHandler(credentials)  # Drive handler

        # Convert time filter to Gmail-compatible query
        EMAIL_QUERY = convert_time_to_query(time_filter)
        update_status(f"Fetching emails with query: '{EMAIL_QUERY}'...")

        # Use the updated extract_email_content method with time_filter for post-processing
        email_data = gmail_handler.extract_email_content(
            query=EMAIL_QUERY,
            max_results=config.MAX_EMAILS,
            time_filter=time_filter,  # Pass the time filter for post-processing
        )
        if not email_data:
            logger.warning("No email data to process.")
            if exit_on_complete:
                sys.exit(
                    0
                )  # Exit gracefully when no data (not an error for scheduled tasks)
            return False, "No email data found for the specified time period."

        update_status(f"Successfully processed {len(email_data)} emails.")

        update_status("Extracting data from emails...")
        df = pd.DataFrame(bigquery_handler.extract_data(email_data))

        if df.empty:
            logger.warning("No data extracted from emails.")
            if exit_on_complete:
                sys.exit(
                    0
                )  # Exit gracefully when no data (not an error for scheduled tasks)
            return False, "No data could be extracted from the emails."

        update_status("Processing attachment details...")
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
        update_status("Standardizing entities and determining transaction direction...")
        final_df = standardize_dataframe(final_df)

        # Format document numbers
        final_df["document_number"] = "'" + final_df["document_number"].astype(str)

        # Upload data to bigquery
        update_status("Uploading data to BigQuery...")
        final_df = bigquery_handler.clean_dataframe_for_bigquery(final_df)
        bigquery_handler.upload_dataframe(
            data=final_df,
            project_id="immortal-0804",
            dataset_id="finance_project",
            table_id="invoice_summarize",
            if_exists="append",  # will append to table if it exists
        )

        # Upload PDF files to Google Drive
        update_status(f"Organizing and uploading PDFs to Drive folder...")
        upload_results = drive_handler.organize_and_upload_pdfs(
            drive_link, final_df, local_pdf_dir="downloads"
        )
        # Check for drive errors
        if "error" in upload_results:
            error_msg = upload_results["error"]
            logger.error(error_msg)
            if exit_on_complete:
                sys.exit(1)  # Exit with error code for scheduled tasks
            return False, error_msg

        update_status(
            f"Upload summary: {upload_results['successful_uploads']} successful, "
            f"{upload_results['failed_uploads']} failed"
        )

        if upload_results["failed_uploads"] > 0:
            logger.warning("Some files failed to upload. Check the log for details.")

        # Get base directory that works in both development and PyInstaller context
        base_dir = os.path.abspath(
            os.path.dirname(sys.executable)
            if getattr(sys, "frozen", False)
            else os.path.dirname(__file__)
        )

        # Initialize with base directory
        local_handler = LocalHandler(base_dir)

        # Organize PDFs from email data
        update_status("Organizing PDFs locally...")
        result = local_handler.organize_and_copy_pdfs(
            "company_files",  # Target directory (will be created if it doesn't exist)
            final_df,  # DataFrame with company and file information
            "downloads",  # Source directory containing PDFs
        )
        # Check for local copy errors
        if "error" in result:
            error_msg = result["error"]
            logger.error(error_msg)
            if exit_on_complete:
                sys.exit(1)  # Exit with error code for scheduled tasks
            return False, error_msg

        success_message = f"Process completed successfully. Uploaded {upload_results['successful_uploads']} files. Copy summary: {result['successful_copies']} successful."
        update_status(success_message)

        if exit_on_complete:
            logger.info("Exiting after successful completion (automated mode)")
            sys.exit(0)  # Success exit code for scheduled tasks

        return True, success_message
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        if exit_on_complete:
            sys.exit(1)  # Error exit code for scheduled tasks
        return False, "Process was interrupted by user."
    except Exception as e:
        error_msg = f"An unexpected error occurred: {str(e)}"
        logger.exception(error_msg)
        if exit_on_complete:
            sys.exit(1)  # Error exit code for scheduled tasks
        return False, error_msg


def create_gui():
    """Create and return the GUI application"""
    if not HAS_TK:
        print("GUI mode requires tkinter which is not available.")
        return None

    root = tk.Tk()
    root.title("Invoice Collection Tool")
    # Increase initial window size to ensure all elements are visible
    root.geometry("600x600")  # Changed from 600x500 to provide more vertical space
    root.minsize(600, 550)  # Set minimum size to prevent hiding elements
    root.resizable(True, True)

    # All variables
    status_var = tk.StringVar(value="Ready")
    drive_var = tk.StringVar(value=config.DRIVE)
    time_var = tk.StringVar(value="1d")
    running = False
    time_choices = ["30m", "1h", "2h", "3h", "6h", "12h", "1d", "2d", "3d", "7d"]
    auto_exit_var = tk.BooleanVar(value=False)  # Added missing variable declaration

    # Task scheduler variables
    task_scheduler_var = tk.BooleanVar(value=False)

    # Center the window
    def center_window():
        root.update_idletasks()
        width = root.winfo_width()
        height = root.winfo_height()
        x = (root.winfo_screenwidth() // 2) - (width // 2)
        y = (root.winfo_screenheight() // 2) - (height // 2)
        root.geometry("{}x{}+{}+{}".format(width, height, x, y))

    # Main frame with padding to ensure content is visible
    main_frame = ttk.Frame(root, padding="20 20 20 20")
    main_frame.pack(fill=tk.BOTH, expand=True)

    # Title
    title_label = ttk.Label(
        main_frame, text="Invoice Collection Tool", font=("Arial", 16, "bold")
    )
    title_label.pack(pady=(0, 15))

    # Input frame
    input_frame = ttk.LabelFrame(
        main_frame, text="Input Parameters", padding="10 10 10 10"
    )
    input_frame.pack(fill=tk.X, pady=(0, 10))

    # Drive link
    drive_frame = ttk.Frame(input_frame)
    drive_frame.pack(fill=tk.X, pady=5)

    drive_label = ttk.Label(drive_frame, text="Google Drive folder link:", width=20)
    drive_label.pack(side=tk.LEFT, padx=(0, 5))

    drive_entry = ttk.Entry(drive_frame, textvariable=drive_var, width=40)
    drive_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

    # Time filter
    time_frame = ttk.Frame(input_frame)
    time_frame.pack(fill=tk.X, pady=5)

    time_label = ttk.Label(time_frame, text="Time filter:", width=20)
    time_label.pack(side=tk.LEFT, padx=(0, 5))

    time_combo = ttk.Combobox(time_frame, textvariable=time_var, values=time_choices)
    time_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)

    # Task Scheduler frame
    scheduler_frame = ttk.LabelFrame(
        main_frame, text="Task Scheduler Configuration", padding="10 10 10 10"
    )
    scheduler_frame.pack(fill=tk.X, pady=(0, 10))

    def create_task_scheduler_flag():
        if task_scheduler_var.get():
            try:
                # Create a flag file that will trigger automated mode when the program starts
                flag_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), "run_from_task.flag"
                )
                with open(flag_path, "w") as f:
                    f.write(f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                messagebox.showinfo(
                    "Task Scheduler Setup",
                    "Flag file created successfully. When the program is launched by Task Scheduler, "
                    "it will automatically process emails with your saved settings and exit.",
                )
            except Exception as e:
                messagebox.showerror(
                    "Error", f"Could not create task scheduler flag: {str(e)}"
                )
        else:
            # Remove the flag file if it exists
            try:
                flag_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), "run_from_task.flag"
                )
                if os.path.exists(flag_path):
                    os.remove(flag_path)
                    messagebox.showinfo(
                        "Task Scheduler Setup", "Automated mode disabled."
                    )
            except Exception as e:
                messagebox.showerror("Error", f"Error removing flag file: {str(e)}")

    # Task scheduler checkbox
    task_check = ttk.Checkbutton(
        scheduler_frame,
        text="Enable Task Scheduler mode (program will run automatically when launched)",
        variable=task_scheduler_var,
        command=create_task_scheduler_flag,
    )
    task_check.pack(pady=5, padx=5, anchor=tk.W)

    # Task scheduler instructions
    task_instr = ttk.Label(
        scheduler_frame,
        text="To use with Task Scheduler:\n"
        "1. Save your settings with the 'Save Settings' button\n"
        "2. Enable the checkbox above\n"
        "3. Set up a task in Windows Task Scheduler to run this program hourly\n"
        "4. The program will automatically process emails and exit",
        justify=tk.LEFT,
        wraplength=550,
    )
    task_instr.pack(pady=5, padx=5, fill=tk.X)

    # Help text
    help_frame = ttk.LabelFrame(main_frame, text="Help", padding="10 10 10 10")
    help_frame.pack(fill=tk.X, pady=(0, 10))

    help_text = """Time filter options:
        - 30m: Emails from the last 30 minutes
        - 1h to 12h: Emails from the last 1-12 hours
        - 1d to 7d: Emails from the last 1-7 days
    """

    help_label = ttk.Label(help_frame, text=help_text, justify=tk.LEFT)
    help_label.pack(fill=tk.X)

    # Status and log area - reduce the expand property
    log_frame = ttk.LabelFrame(main_frame, text="Status", padding="10 10 10 10")
    log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

    log_text = scrolledtext.ScrolledText(
        log_frame, wrap=tk.WORD, height=6
    )  # Reduced height
    log_text.pack(fill=tk.BOTH, expand=True)
    log_text.config(state=tk.DISABLED)

    # Bottom buttons - use a fixed height to ensure visibility
    button_frame = ttk.Frame(main_frame)
    button_frame.pack(fill=tk.X, pady=(0, 5))

    status_label = ttk.Label(button_frame, textvariable=status_var, foreground="blue")
    status_label.pack(side=tk.LEFT)

    # Progress bar
    progress = ttk.Progressbar(main_frame, mode="indeterminate")
    progress.pack(fill=tk.X, pady=(0, 10))  # Added vertical padding

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
            if not auto_exit_var.get():  # Only show dialog if not auto-exit
                messagebox.showinfo("Success", message)
        else:
            status_var.set("Process failed")
            if not auto_exit_var.get():  # Only show dialog if not auto-exit
                messagebox.showerror("Error", message)

        update_log(message)

        # Auto-exit if needed
        if auto_exit_var.get():
            update_log("Auto-exit enabled. Exiting in 3 seconds...")
            root.after(3000, root.destroy)  # Exit after 3 seconds

    # Function to run the process
    def run_process():
        nonlocal running
        if running:
            messagebox.showinfo("Already Running", "Process is already running.")
            return

        # Get input values
        drive_link = drive_var.get() or config.DRIVE
        time_filter = time_var.get()

        # Update UI
        running = True
        run_button.config(state=tk.DISABLED)
        update_log(f"Starting process with time filter: {time_filter}")
        progress.start()

        # Run in a separate thread to keep UI responsive
        def threaded_run():
            success, message = run_invoice_collection(
                drive_link=drive_link,
                time_filter=time_filter,
                status_callback=update_log,
                exit_on_complete=False,  # Don't exit directly, let the GUI handle it
            )

            # Update UI with result
            root.after(0, lambda: process_complete(success, message))

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

    # Create buttons - ensure they're visible with fixed height and padding
    buttons_container = ttk.Frame(button_frame, height=40)  # Fixed height for buttons
    buttons_container.pack(side=tk.RIGHT)

    run_button = ttk.Button(buttons_container, text="Run Process", command=run_process)
    run_button.pack(side=tk.RIGHT, padx=5)

    exit_button = ttk.Button(buttons_container, text="Exit", command=on_close)
    exit_button.pack(side=tk.RIGHT)

    # Save settings button
    def save_settings():
        try:
            # Use absolute path for settings to work with Task Scheduler
            settings_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "settings.txt"
            )
            with open(settings_path, "w") as f:
                f.write(f"DRIVE_LINK={drive_var.get()}\n")
                f.write(f"TIME_FILTER={time_var.get()}\n")
            update_log("Settings saved successfully")
            messagebox.showinfo(
                "Settings Saved",
                "Your settings have been saved successfully. These settings will be used when "
                "the program runs automatically from Task Scheduler.",
            )
        except Exception as e:
            update_log(f"Error saving settings: {str(e)}")
            messagebox.showerror("Error", f"Failed to save settings: {str(e)}")

    save_button = ttk.Button(
        buttons_container, text="Save Settings", command=save_settings
    )
    save_button.pack(side=tk.RIGHT, padx=5)

    # Test Task Scheduler button
    def test_task_mode():
        # Create a temporary flag file
        flag_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "run_from_task.flag"
        )
        try:
            with open(flag_path, "w") as f:
                f.write(f"Test run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            msg = (
                "Task Scheduler test mode will be activated.\n\n"
                "The program will close and reopen in automated mode.\n\n"
                "This simulates what will happen when Task Scheduler launches the program."
            )

            if messagebox.askyesno("Test Task Scheduler Mode", msg):
                # Get the executable path
                if getattr(sys, "frozen", False):
                    app_path = sys.executable
                else:
                    app_path = sys.argv[0]

                # Launch a new instance of the program
                import subprocess

                subprocess.Popen([app_path])

                # Close this instance
                root.destroy()
        except Exception as e:
            messagebox.showerror(
                "Error", f"Could not test Task Scheduler mode: {str(e)}"
            )

    test_button = ttk.Button(
        buttons_container, text="Test Task Mode", command=test_task_mode
    )
    test_button.pack(side=tk.RIGHT, padx=5)

    # Try to load saved settings
    try:
        settings_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "settings.txt"
        )
        if os.path.exists(settings_path):
            with open(settings_path, "r") as f:
                for line in f:
                    if "=" in line:
                        key, value = line.strip().split("=", 1)
                        if key == "DRIVE_LINK" and value:
                            drive_var.set(value)
                        elif key == "TIME_FILTER" and value:
                            time_var.set(value)
    except Exception as e:
        logger.warning(f"Error loading settings: {str(e)}")

    # Check if task scheduler mode is enabled
    try:
        flag_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "run_from_task.flag"
        )
        task_scheduler_var.set(os.path.exists(flag_path))
    except Exception:
        pass

    # Center the window after all widgets are added
    center_window()

    return root


def main():
    """Main entry point with simplified Task Scheduler support"""
    # Set up logging
    logger = setup_logger()
    logger.info("Application started")

    # Check if we're running in Task Scheduler mode
    # When running from Task Scheduler, we'll use a file flag
    is_task_scheduler = False
    task_flag_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "run_from_task.flag"
    )

    # Check for the task scheduler flag file
    if os.path.exists(task_flag_file):
        is_task_scheduler = True
        try:
            # Remove the flag file so it doesn't trigger again until next scheduled run
            os.remove(task_flag_file)
            logger.info("Running in Task Scheduler mode (flag file detected)")
        except Exception as e:
            logger.warning(f"Error removing task flag file: {str(e)}")

    # Load saved settings regardless of mode
    drive_link = config.DRIVE
    time_filter = "1h"  # Default for scheduled tasks

    try:
        settings_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "settings.txt"
        )
        if os.path.exists(settings_path):
            with open(settings_path, "r") as f:
                for line in f:
                    if "=" in line:
                        key, value = line.strip().split("=", 1)
                        if key == "DRIVE_LINK" and value:
                            drive_link = value
                        elif key == "TIME_FILTER" and value:
                            time_filter = value
            logger.info(
                f"Loaded settings from file: drive_link={drive_link}, time_filter={time_filter}"
            )
    except Exception as e:
        logger.warning(f"Error loading settings: {str(e)}")

    # If running from Task Scheduler, run without GUI and exit when done
    if is_task_scheduler:
        logger.info(f"Starting in automated mode with time filter: {time_filter}")
        success, message = run_invoice_collection(
            drive_link=drive_link,
            time_filter=time_filter,
            exit_on_complete=True,  # Exit when done
        )
        # This will never execute because run_invoice_collection will exit the program
        return 0 if success else 1

    # Otherwise, start in GUI mode
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

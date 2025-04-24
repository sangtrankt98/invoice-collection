#!/usr/bin/env python3
"""
Invoice Collection Main Script
Automates the process of collecting invoice data from emails and uploading to BigQuery
"""
import threading
import traceback
import sys
import pandas as pd
import logging
from datetime import datetime
from utils.auth import GoogleAuthenticator
from utils.gmail_handler import GmailHandler
from utils.drive_handler import DriveHandler
from utils.bigquery_uploader import BigQueryUploader
from utils.logger_setup import setup_logger
import config
import tkinter as tk
from tkinter import scrolledtext
from tkinter import messagebox


class TextHandler(logging.Handler):
    """Custom logging handler that redirects logs to a Tkinter Text widget"""

    def __init__(self, text_widget):
        logging.Handler.__init__(self)
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)

        def append():
            self.text_widget.configure(state="normal")
            self.text_widget.insert(tk.END, msg + "\n")
            self.text_widget.see(tk.END)  # Auto-scroll to the bottom
            self.text_widget.configure(state="disabled")

        # Schedule the update to avoid thread issues with Tkinter
        self.text_widget.after(0, append)


class InvoiceCollectorGUI:
    def __init__(self, root):
        """Initialize the GUI components"""
        self.root = root
        self.root.title("Invoice Collector")
        self.root.geometry("600x500")  # Increased size to accommodate log panel

        self._create_widgets()
        self._setup_logger()

    def _create_widgets(self):
        """Create and layout all GUI widgets"""
        # Input Frame
        input_frame = tk.Frame(self.root)
        input_frame.pack(fill=tk.X, padx=10, pady=10)

        # Row 0: Label for Google Drive Link
        tk.Label(input_frame, text="Google Drive Folder Link:").grid(
            row=0, column=0, sticky="w", padx=10, pady=5
        )

        # Row 0: Entry box for Drive link
        self.entry_link = tk.Entry(input_frame, width=60)
        self.entry_link.grid(row=0, column=1, columnspan=2, padx=10, pady=5)

        # Row 1: Helper text below the entry
        helper_text = "Paste a Google Drive folder link where files will be organized"
        tk.Label(
            input_frame,
            text=helper_text,
            fg="gray",
            font=("Arial", 8),
            wraplength=400,
            justify="left",
        ).grid(row=1, column=1, columnspan=2, sticky="w", padx=10)

        # Row 2: Start Date
        tk.Label(input_frame, text="Start Date (YYYY-MM-DD):").grid(
            row=2, column=0, sticky="w", padx=10, pady=5
        )
        self.entry_date_after = tk.Entry(input_frame, width=20)
        self.entry_date_after.grid(row=2, column=1, padx=10, pady=5)

        # Row 3: End Date
        tk.Label(input_frame, text="End Date (YYYY-MM-DD):").grid(
            row=3, column=0, sticky="w", padx=10, pady=5
        )
        self.entry_date_before = tk.Entry(input_frame, width=20)
        self.entry_date_before.grid(row=3, column=1, padx=10, pady=5)

        # Row 4: Submit Button
        tk.Button(input_frame, text="Submit", command=self.on_submit).grid(
            row=4, column=1, pady=10
        )

        # Log Frame
        log_frame = tk.Frame(self.root)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Log Display Label
        tk.Label(log_frame, text="Processing Log:").pack(anchor="w")

        # Log Text Area with Scrollbar
        self.log_display = scrolledtext.ScrolledText(log_frame, height=15)
        self.log_display.pack(fill=tk.BOTH, expand=True)
        self.log_display.configure(state="disabled")  # Make it read-only

        # Status Bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        status_bar = tk.Label(
            self.root, textvariable=self.status_var, bd=1, relief=tk.SUNKEN, anchor=tk.W
        )
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def _setup_logger(self):
        """Configure logger to output to both file and GUI text widget"""
        # Get the root logger
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        # Clear existing handlers to avoid duplicates
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Add file handler
        file_handler = logging.FileHandler("invoice_collector.log", "w", "utf-8")
        file_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)

        # Add GUI text handler
        text_handler = TextHandler(self.log_display)
        text_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        text_handler.setFormatter(text_format)
        self.logger.addHandler(text_handler)

        self.logger.info("Application started")

    def update_status(self, message):
        """Update the status bar message"""
        self.status_var.set(message)
        self.root.update_idletasks()  # Force update of the UI

    def on_submit(self):
        """Handle submit button click"""
        # Update UI state
        self.update_status("Processing...")

        drive_link = self.entry_link.get()
        date_after = self.entry_date_after.get()
        date_before = self.entry_date_before.get()

        # Clear log display
        self.log_display.configure(state="normal")
        self.log_display.delete(1.0, tk.END)
        self.log_display.configure(state="disabled")

        # Validate inputs
        if not drive_link.strip():
            messagebox.showerror(
                "Missing Input", "Please enter the Google Drive folder link"
            )
            self.update_status("Ready")
            return

        try:
            datetime.strptime(date_after, "%Y-%m-%d")
            datetime.strptime(date_before, "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("Invalid Date", "Use format YYYY-MM-DD")
            self.update_status("Ready")
            return

        self.logger.info(
            f"Starting process with date range: {date_after} to {date_before}"
        )

        # Run main in background to avoid freezing the GUI
        thread = threading.Thread(
            target=self.safe_run_main,
            args=(drive_link, date_after, date_before),
            daemon=True,
        )
        thread.start()

    def safe_run_main(self, master_drive, date_after, date_before):
        """Run the main function safely with error handling"""
        try:
            # Get the result status from the processing function
            success, message = self.run_invoice_collection(
                master_drive, date_after, date_before
            )

            if success:
                self.logger.info("Process completed successfully")
                self.root.after(
                    0,
                    lambda: [
                        messagebox.showinfo(
                            "Success", "Invoice processing completed successfully."
                        ),
                        self.update_status("Ready"),
                    ],
                )
            else:
                self.logger.error(f"Process failed: {message}")
                self.root.after(
                    0,
                    lambda: [
                        messagebox.showerror(
                            "Process Failed", f"Invoice processing failed:\n{message}"
                        ),
                        self.update_status("Ready"),
                    ],
                )

        except Exception as e:
            error_traceback = traceback.format_exc()
            self.logger.error(f"Unexpected error in GUI thread: {str(e)}")
            self.logger.debug(error_traceback)
            self.root.after(
                0,
                lambda: [
                    messagebox.showerror(
                        "Error", f"An unexpected error occurred:\n{str(e)}"
                    ),
                    self.update_status("Ready"),
                ],
            )

    def run_invoice_collection(self, drive_link, start_str, end_str):
        """
        Main function to orchestrate the invoice collection process
        Returns a tuple: (success_flag, message)
        """
        try:
            self.logger.info("Starting invoice collection process")
            self.logger.info("Authenticating with Google services...")
            authenticator = GoogleAuthenticator(config.SCOPES)
            credentials = authenticator.get_credentials()

            self.logger.info("Initializing service handlers...")
            gmail_handler = GmailHandler(
                credentials, config.OPENAI, DriveHandler(credentials)
            )
            bigquery_uploader = BigQueryUploader(credentials)
            drive_handler = DriveHandler(credentials)  # Drive handler

            try:
                start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
                end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
                self.logger.info(f"Date range: from {start_date} to {end_date}")
            except ValueError:
                return False, "Invalid date format. Please use YYYY-MM-DD."
            # from:sang.tranphuoc@ninjavan.co
            EMAIL_QUERY = f'label:"Email Test" after:{start_date.strftime("%Y/%m/%d")} before:{end_date.strftime("%Y/%m/%d")}'
            self.logger.info(f"Fetching emails with query: '{EMAIL_QUERY}'...")

            email_data = gmail_handler.extract_email_content(
                query=EMAIL_QUERY, max_results=config.MAX_EMAILS
            )
            if not email_data:
                self.logger.warning("No email data to process.")
                return False, "No email data found for the specified date range."

            self.logger.info(f"Successfully processed {len(email_data)} emails.")

            self.logger.info("Extracting data from emails...")
            df = pd.DataFrame(bigquery_uploader.extract_data(email_data))

            if df.empty:
                self.logger.warning("No data extracted from emails.")
                return False, "No data could be extracted from the emails."

            self.logger.info("Processing attachment details...")
            df_exploded = df.explode("attachment_details")
            attachment_df = pd.json_normalize(df_exploded["attachment_details"])
            final_df = pd.concat(
                [
                    df_exploded.drop(columns=["attachment_details"]).reset_index(
                        drop=True
                    ),
                    attachment_df.reset_index(drop=True),
                ],
                axis=1,
            )

            self.logger.info("Saving data to CSV file...")

            def get_most_common_or_unverified(x):
                if x.dropna().empty:
                    return "Unverified"
                return x.value_counts().idxmax()

            final_df["company_name"] = final_df.groupby("message_id")[
                "company_name"
            ].transform(get_most_common_or_unverified)
            # final_df["invoice_number"] = final_df["invoice_number"].astype(str)
            final_df["invoice_number"] = "'" + final_df["invoice_number"].astype(str)
            final_df.to_csv(
                "flattened_attachments.csv", index=False, encoding="utf-8-sig"
            )
            self.logger.info(f"CSV saved successfully with {len(final_df)} rows.")

            # Upload PDF files to Google Drive
            self.logger.info(
                f"Organizing and uploading PDFs to Drive folder: {drive_link}"
            )
            upload_results = drive_handler.organize_and_upload_pdfs(
                drive_link, final_df, local_pdf_dir="downloads"
            )

            # Check for drive errors
            if "error" in upload_results:
                return False, upload_results["error"]

            self.logger.info(
                f"Upload summary: {upload_results['successful_uploads']} successful, "
                f"{upload_results['failed_uploads']} failed"
            )

            if upload_results["failed_uploads"] > 0:
                self.logger.warning(
                    "Some files failed to upload. Check the log for details."
                )

            return (
                True,
                f"Process completed successfully. Uploaded {upload_results['successful_uploads']} files.",
            )

        except KeyboardInterrupt:
            self.logger.info("Process interrupted by user")
            return False, "Process was interrupted by user."
        except Exception as e:
            error_msg = f"An unexpected error occurred: {str(e)}"
            self.logger.exception(error_msg)
            return False, error_msg


# Main execution part
if __name__ == "__main__":
    root = tk.Tk()
    app = InvoiceCollectorGUI(root)
    root.mainloop()

#!/usr/bin/env python3
"""
Invoice Collection Main Script
Automates the process of collecting invoice data from emails and uploading to BigQuery
"""
import threading
import traceback
import sys
import pandas as pd
from datetime import datetime
from utils.auth import GoogleAuthenticator
from utils.gmail_handler import GmailHandler
from utils.bigquery_uploader import BigQueryUploader
from utils.logger_setup import setup_logger
import config
import tkinter as tk
from tkinter import messagebox


class InvoiceCollectorGUI:
    def __init__(self, root):
        """Initialize the GUI components"""
        self.root = root
        self.root.title("Invoice Collector")
        self.root.geometry("400x220")
        self.logger = setup_logger()

        self._create_widgets()

    def _create_widgets(self):
        """Create and layout all GUI widgets"""
        # Master Drive Link row
        tk.Label(self.root, text="Master Drive Link:").grid(
            row=0, column=0, sticky="w", padx=10, pady=5
        )
        self.entry_link = tk.Entry(self.root, width=40)
        self.entry_link.grid(row=0, column=1, padx=10, pady=5)

        # Start Date row
        tk.Label(self.root, text="Start Date (YYYY-MM-DD):").grid(
            row=1, column=0, sticky="w", padx=10, pady=5
        )
        self.entry_date_after = tk.Entry(self.root, width=20)
        self.entry_date_after.grid(row=1, column=1, padx=10, pady=5)

        # End Date row
        tk.Label(self.root, text="End Date (YYYY-MM-DD):").grid(
            row=2, column=0, sticky="w", padx=10, pady=5
        )
        self.entry_date_before = tk.Entry(self.root, width=20)
        self.entry_date_before.grid(row=2, column=1, padx=10, pady=5)

        # Submit button
        tk.Button(self.root, text="Submit", command=self.on_submit).grid(
            row=3, column=1, pady=20
        )

    def on_submit(self):
        """Handle submit button click"""
        master_drive = self.entry_link.get()
        date_after = self.entry_date_after.get()
        date_before = self.entry_date_before.get()

        # Validate inputs
        if not master_drive.strip():
            messagebox.showerror("Missing Input", "Please enter the master drive link")
            return

        try:
            datetime.strptime(date_after, "%Y-%m-%d")
            datetime.strptime(date_before, "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("Invalid Date", "Use format YYYY-MM-DD")
            return

        # Run main in background to avoid freezing the GUI
        thread = threading.Thread(
            target=self.safe_run_main,
            args=(master_drive, date_after, date_before),
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
                messagebox.showinfo(
                    "Success", "Invoice processing completed successfully."
                )
            else:
                messagebox.showerror(
                    "Process Failed", f"Invoice processing failed:\n{message}"
                )

        except Exception as e:
            error_traceback = traceback.format_exc()
            self.logger.error(f"Unexpected error in GUI thread: {error_traceback}")
            print(error_traceback)  # Log to console
            messagebox.showerror("Error", f"An unexpected error occurred:\n{str(e)}")

    def run_invoice_collection(self, master_drive, start_str, end_str):
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
            gmail_handler = GmailHandler(credentials, config.OPENAI)
            bigquery_uploader = BigQueryUploader(credentials)

            try:
                start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
                end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
                print(f"Date range: from {start_date} to {end_date}")
            except ValueError:
                return False, "Invalid date format. Please use YYYY-MM-DD."

            EMAIL_QUERY = f'from:sang.tranphuoc@ninjavan.co label:"Email Test" after:{start_date.strftime("%Y/%m/%d")} before:{end_date.strftime("%Y/%m/%d")}'
            self.logger.info(f"Fetching emails with query: '{EMAIL_QUERY}'...")

            email_data = gmail_handler.extract_email_content(
                query=EMAIL_QUERY, max_results=config.MAX_EMAILS
            )
            if not email_data:
                self.logger.warning("No email data to process.")
                return False, "No email data found for the specified date range."

            self.logger.info(f"Processed {len(email_data)} emails.")

            df = pd.DataFrame(bigquery_uploader.extract_data(email_data))

            if df.empty:
                self.logger.warning("No data extracted from emails.")
                return False, "No data could be extracted from the emails."

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

            final_df.to_csv(
                "flattened_attachments.csv", index=False, encoding="utf-8-sig"
            )

            self.logger.info("Data successfully extracted and saved to CSV.")
            return True, "Process completed successfully"

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

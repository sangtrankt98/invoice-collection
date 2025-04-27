#!/usr/bin/env python3
"""
Transaction Reports Generator
Generates Báo Cáo Mua Vào (Incoming) and Báo Cáo Bán Ra (Outgoing) reports from processed transaction data
"""
import pandas as pd
import os
from datetime import datetime
import openpyxl
from openpyxl.styles import Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import shutil
import logging


class TransactionReportGenerator:
    """
    Generates separate incoming and outgoing transaction reports
    based on the direction attribute in the processed data
    """

    def __init__(self, input_file="flattened_attachments.csv", template_file=None):
        """
        Initialize with input data file and optional template

        Args:
            input_file (str): Path to the CSV file containing transaction data
            template_file (str, optional): Path to Excel template for reports
        """
        self.input_file = input_file
        self.template_file = template_file
        self.data = None
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """Setup basic logger"""
        logger = logging.getLogger("TransactionReports")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def load_data(self):
        """Load and validate the transaction data"""
        try:
            self.logger.info(f"Loading data from {self.input_file}")
            self.data = pd.read_csv(self.input_file, encoding="utf-8-sig")

            # Check if direction column exists
            if "direction" not in self.data.columns:
                self.logger.error("Missing 'direction' column in the data")
                return False

            # Fill any missing directions with 'UNKNOWN' - fixed to avoid FutureWarning
            # Instead of using chained assignment with inplace=True
            self.data = (
                self.data.copy()
            )  # Create a copy to ensure we're modifying the original
            self.data["direction"] = self.data["direction"].fillna("UNKNOWN")

            # Basic validation
            self.logger.info(f"Loaded {len(self.data)} transactions")
            self.logger.info(
                f"Directions found: {self.data['direction'].value_counts().to_dict()}"
            )

            return True
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            return False

    def generate_incoming_report(self, output_file="Bao_Cao_Mua_Vao.xlsx"):
        """
        Generate the incoming transactions report (Báo Cáo Mua Vào)

        Args:
            output_file (str): Path to save the generated report

        Returns:
            bool: Success or failure
        """
        if self.data is None:
            if not self.load_data():
                return False

        try:
            # Filter for incoming transactions
            incoming_df = self.data[self.data["direction"] == "INCOMING"].copy()
            self.logger.info(f"Processing {len(incoming_df)} incoming transactions")

            if len(incoming_df) == 0:
                self.logger.warning("No incoming transactions found")
                return False

            # Sort by date
            if "date" in incoming_df.columns:
                incoming_df["date"] = pd.to_datetime(
                    incoming_df["date"], errors="coerce"
                )
                incoming_df.sort_values("date", inplace=True)

            # Create report
            return self._create_report(incoming_df, output_file, report_type="MUA VÀO")
        except Exception as e:
            self.logger.error(f"Error generating incoming report: {str(e)}")
            return False

    def generate_outgoing_report(self, output_file="Bao_Cao_Ban_Ra.xlsx"):
        """
        Generate the outgoing transactions report (Báo Cáo Bán Ra)

        Args:
            output_file (str): Path to save the generated report

        Returns:
            bool: Success or failure
        """
        if self.data is None:
            if not self.load_data():
                return False

        try:
            # Filter for outgoing transactions
            outgoing_df = self.data[self.data["direction"] == "OUTGOING"].copy()
            self.logger.info(f"Processing {len(outgoing_df)} outgoing transactions")

            if len(outgoing_df) == 0:
                self.logger.warning("No outgoing transactions found")
                return False

            # Sort by date
            if "date" in outgoing_df.columns:
                outgoing_df["date"] = pd.to_datetime(
                    outgoing_df["date"], errors="coerce"
                )
                outgoing_df.sort_values("date", inplace=True)

            # Create report
            return self._create_report(outgoing_df, output_file, report_type="BÁN RA")
        except Exception as e:
            self.logger.error(f"Error generating outgoing report: {str(e)}")
            return False

    def _create_report(self, df, output_file, report_type):
        """
        Create an Excel report with proper formatting

        Args:
            df: DataFrame containing the filtered transaction data
            output_file: Path to save the output Excel file
            report_type: "MUA VÀO" or "BÁN RA"

        Returns:
            bool: Success or failure
        """
        try:
            # Get entity name for the report title
            entity_name = "CÔNG TY TNHH"
            if "entity_name" in df.columns and not df["entity_name"].empty:
                entity_names = df["entity_name"].dropna().unique()
                if len(entity_names) > 0:
                    entity_name = entity_names[0]

            # Get date range
            date_range_text = ""
            if "date" in df.columns:
                min_date = df["date"].min()
                max_date = df["date"].max()
                if pd.notna(min_date) and pd.notna(max_date):
                    if isinstance(min_date, pd.Timestamp) and isinstance(
                        max_date, pd.Timestamp
                    ):
                        date_range_text = f"Từ ngày {min_date.strftime('%d/%m/%Y')} đến ngày {max_date.strftime('%d/%m/%Y')}"

            # If using template, copy it first
            if self.template_file and os.path.exists(self.template_file):
                self.logger.info(f"Using template: {self.template_file}")
                shutil.copy2(self.template_file, output_file)
                wb = openpyxl.load_workbook(output_file)

                # Use the first sheet
                if len(wb.sheetnames) > 0:
                    sheet = wb.active

                    # Update company name and title in template if cells A1 and A2 exist
                    if "A1" in sheet:
                        sheet["A1"] = entity_name
                    if "A2" in sheet:
                        sheet["A2"] = f"BÁO CÁO {report_type}"
                    if "A3" in sheet:
                        # Use the actual date range if available
                        if date_range_text:
                            sheet["A3"] = date_range_text
                        else:
                            # Default to current month/year
                            sheet["A3"] = (
                                f"Kỳ báo cáo: {datetime.now().strftime('%m/%Y')}"
                            )

                # Find the data start row in template (assuming headers are already set)
                data_start_row = None
                for row in range(1, 20):  # Search in the first 20 rows
                    if (
                        sheet.cell(row=row, column=1).value == "STT"
                        or sheet.cell(row=row, column=1).value == "Stt"
                    ):
                        data_start_row = row + 1  # Start data one row after headers
                        break

                if not data_start_row:
                    data_start_row = 6  # Default if not found
            else:
                # Create new workbook from scratch
                self.logger.info("Creating new workbook from scratch")
                wb = openpyxl.Workbook()
                sheet = wb.active
                sheet.title = f"Báo Cáo {report_type}"

                # Add header and title
                sheet["A1"] = entity_name
                sheet["A2"] = f"BÁO CÁO {report_type}"

                # Use date range if available
                if date_range_text:
                    sheet["A3"] = date_range_text
                else:
                    sheet["A3"] = f"Kỳ báo cáo: {datetime.now().strftime('%m/%Y')}"

                # Format header
                sheet["A1"].font = Font(size=14, bold=True)
                sheet["A2"].font = Font(size=16, bold=True)
                sheet["A3"].font = Font(italic=True)

                # Create table headers
                headers = [
                    "STT",
                    "Ngày HĐ",
                    "Số HĐ",
                    "Mã số thuế",
                    "Tên đối tác",
                    "Tên hàng hóa/dịch vụ",
                    "Thành tiền",
                    "Thuế suất",
                    "Tiền thuế",
                    "Tổng tiền",
                ]

                header_row = 5
                for col, header in enumerate(headers, start=1):
                    cell = sheet.cell(row=header_row, column=col)
                    cell.value = header
                    cell.font = Font(bold=True)
                    cell.alignment = Alignment(horizontal="center", vertical="center")

                    # Add borders
                    thin_border = Border(
                        left=Side(style="thin"),
                        right=Side(style="thin"),
                        top=Side(style="thin"),
                        bottom=Side(style="thin"),
                    )
                    cell.border = thin_border

                data_start_row = 6  # Row after headers

            # Map DataFrame columns to Excel columns
            column_map = {
                "date": "Ngày HĐ",
                "document_number": "Số HĐ",
                "counterparty_tax_number": "Mã số thuế",
                "counterparty_name": "Tên đối tác",
                "description": "Tên hàng hóa/dịch vụ",
                "amount_before_tax": "Thành tiền",
                "tax_rate": "Thuế suất",
                "tax_amount": "Tiền thuế",
                "total_amount": "Tổng tiền",
            }

            # Replace column headers in DataFrame for easier mapping
            renamed_df = df.rename(
                columns={k: v for k, v in column_map.items() if k in df.columns}
            )

            # Add STT (row numbers)
            renamed_df.insert(0, "STT", range(1, len(renamed_df) + 1))

            # Format dates if present
            if (
                "Ngày HĐ" in renamed_df.columns
                and pd.api.types.is_datetime64_any_dtype(renamed_df["Ngày HĐ"])
            ):
                renamed_df["Ngày HĐ"] = renamed_df["Ngày HĐ"].dt.strftime("%d/%m/%Y")

            # Clean data - handle NaN values
            for col in renamed_df.columns:
                renamed_df[col] = renamed_df[col].fillna("")

            # Find headers in the sheet
            headers = []
            header_col_map = {}

            # Look for headers either in the template or use our predefined ones
            for col in range(1, 20):  # Search first 20 columns
                header_value = sheet.cell(row=data_start_row - 1, column=col).value
                if header_value:
                    headers.append(header_value)
                    header_col_map[header_value] = col

            # If no headers found, use default ones
            if not headers:
                headers = [
                    "STT",
                    "Ngày HĐ",
                    "Số HĐ",
                    "Mã số thuế",
                    "Tên đối tác",
                    "Tên hàng hóa/dịch vụ",
                    "Thành tiền",
                    "Thuế suất",
                    "Tiền thuế",
                    "Tổng tiền",
                ]
                # Create mapping from header name to column number
                for idx, header in enumerate(headers, start=1):
                    header_col_map[header] = idx

            # Write data rows
            for r_idx, row in enumerate(renamed_df.iterrows(), start=data_start_row):
                row_data = row[1]  # row[0] is index

                for header in headers:
                    if header in row_data and header in header_col_map:
                        col = header_col_map[header]
                        cell = sheet.cell(row=r_idx, column=col)
                        value = row_data[header]

                        # Format certain columns
                        if header in ["Thành tiền", "Tiền thuế", "Tổng tiền"]:
                            if isinstance(value, (int, float)):
                                cell.value = value
                                cell.number_format = "#,##0"
                            else:
                                try:
                                    cell.value = float(value) if value else 0
                                    cell.number_format = "#,##0"
                                except (ValueError, TypeError):
                                    cell.value = value
                        elif header == "Thuế suất":
                            if isinstance(value, (int, float)):
                                cell.value = (
                                    value / 100
                                )  # Convert to decimal for percentage
                                cell.number_format = "0%"
                            else:
                                try:
                                    cell.value = float(value) / 100 if value else 0
                                    cell.number_format = "0%"
                                except (ValueError, TypeError):
                                    cell.value = value
                        else:
                            cell.value = value

                        # Add borders
                        thin_border = Border(
                            left=Side(style="thin"),
                            right=Side(style="thin"),
                            top=Side(style="thin"),
                            bottom=Side(style="thin"),
                        )
                        cell.border = thin_border

                        # Add alignment
                        if header in ["STT", "Ngày HĐ", "Thuế suất"]:
                            cell.alignment = Alignment(horizontal="center")
                        elif header in ["Thành tiền", "Tiền thuế", "Tổng tiền"]:
                            cell.alignment = Alignment(horizontal="right")

            # Add summary row
            summary_row = data_start_row + len(renamed_df)
            sheet.cell(row=summary_row, column=1).value = "Tổng cộng:"
            sheet.cell(row=summary_row, column=1).font = Font(bold=True)

            # Add sum formulas for amount columns
            amount_columns = {"Thành tiền": None, "Tiền thuế": None, "Tổng tiền": None}

            # Find the column indexes for amount columns
            for header, col in header_col_map.items():
                if header in amount_columns:
                    amount_columns[header] = col

            # Add formulas to the summary row
            for header, col in amount_columns.items():
                if col is not None:
                    col_letter = get_column_letter(col)
                    formula = f"=SUM({col_letter}{data_start_row}:{col_letter}{summary_row-1})"
                    cell = sheet.cell(row=summary_row, column=col)
                    cell.value = formula
                    cell.font = Font(bold=True)
                    cell.number_format = "#,##0"

            # Save the workbook
            wb.save(output_file)
            self.logger.info(f"Report saved as {output_file}")
            return True

        except Exception as e:
            self.logger.error(f"Error creating report: {str(e)}")
            return False

    def generate_all_reports(self, base_folder="reports"):
        """
        Generate both incoming and outgoing reports

        Args:
            base_folder (str): Base folder to save reports

        Returns:
            bool: Success or failure
        """
        try:
            if not os.path.exists(base_folder):
                os.makedirs(base_folder)
                self.logger.info(f"Created reports folder: {base_folder}")

            # Generate both reports
            incoming_file = os.path.join(base_folder, "Bao_Cao_Mua_Vao.xlsx")
            outgoing_file = os.path.join(base_folder, "Bao_Cao_Ban_Ra.xlsx")

            incoming_success = self.generate_incoming_report(incoming_file)
            outgoing_success = self.generate_outgoing_report(outgoing_file)

            if incoming_success or outgoing_success:
                self.logger.info("Report generation completed")
                return True
            else:
                self.logger.error("Failed to generate any reports")
                return False

        except Exception as e:
            self.logger.error(f"Error generating reports: {str(e)}")
            return False

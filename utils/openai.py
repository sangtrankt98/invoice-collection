import openai
import base64
import pandas as pd

import fitz  # PyMuPDF
import pdfplumber
import os
from PIL import Image
import logging
import ast
import json
import re
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="pdfplumber")
warnings.filterwarnings("ignore", category=UserWarning, module="pdfminer")
# Set up logger
logger = logging.getLogger("invoice_collection.invoice")


class InvoiceExtractor:
    """
    A class to extract information from PDF invoices using OpenAI API.
    """

    def __init__(self, api_key):
        """
        Initialize the InvoiceExtractor with OpenAI API key.

        Args:
            api_key (str): OpenAI API key
        """
        self.api_key = api_key
        openai.api_key = api_key
        self.temp_folder = "temp_images"

        # Create temp folder if it doesn't exist
        if not os.path.exists(self.temp_folder):
            os.makedirs(self.temp_folder)

    def extract_data_from_image(self, image_path):
        """
        Extract invoice data from an image using OpenAI API.

        Args:
            image_path (str): Path to the image file

        Returns:
            dict: Extracted invoice data
        """
        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")

        response = openai.chat.completions.create(
            model="gpt-4-vision-preview",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": """
                            You are an expert in extracting structured data from scanned or text-based invoices. The invoice contains vietnamese and english.
                            Always return clean JSON only. Do not hallucinate.
                            If you are unsure about a field, leave it as an empty string.
                            Extract the following fields from the invoice: invoice_number, date, company_name, company_tax_number, seller, total_amount.
                                - Return the output as a valid JSON object only, no markdown or code blocks.
                                - Use double quotes (") for all keys and string values.
                                - Format invoice_number as a **string** to preserve leading zeroes.
                                - The date must be formatted as yyyy-mm-dd and usually >= 2024.
                                - company_name should be the **name of the buyer company (the one purchasing the product)** — not the seller
                                - company_tax_number as a **string** to preserve leading zeroes.
                                - seller the name of company (the one sell product)
                                - total_amount should be a **number without any commas, currency symbols, or formatting**.
                            """,
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{image_data}"
                            },
                        },
                    ],
                }
            ],
            max_tokens=500,
        )

        # Parse the response to get a dictionary
        result_str = response.choices[0].message.content

        try:
            # Step 1: Remove code fences
            result_str = result_str.strip().strip("`")
            result_str = re.sub(r"^```(?:json|python)?", "", result_str).strip()
            result_str = re.sub(r"```$", "", result_str).strip()

            # Step 2: Ensure the string ends with a closing curly brace
            if result_str.count("{") > result_str.count("}"):
                result_str += "}"

            # Step 3: Normalize quotes if needed
            if result_str.startswith("{'") or result_str.startswith("['"):
                result_str = result_str.replace("'", '"')

            # Step 4: Try parsing with json first
            try:
                result_dict = json.loads(result_str)
            except json.JSONDecodeError:
                result_dict = ast.literal_eval(result_str)

            if not isinstance(result_dict, dict):
                raise ValueError("Parsed result is not a dictionary")

            return result_dict

        except Exception as e:
            logger.error(f"Error parsing result: {e}")
            logger.error(f"Original result: {result_str}")
            return {"error": "Failed to parse result"}

    def extract_data_from_pdf_text(self, pdf_path):
        """
        Extract invoice data from a PDF using pdfplumber + OpenAI API (text-based approach).

        Args:
            pdf_path (str): Path to the PDF file

        Returns:
            dict: Extracted invoice data
        """

        try:
            # Extract text from PDF
            with pdfplumber.open(pdf_path) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages)

            if not text.strip():
                raise ValueError("No text extracted from PDF")

            response = openai.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {
                        "role": "user",
                        "content": f"""
                        You are an expert in extracting structured data from scanned or text-based invoices. The invoice contains Vietnamese and English.
                        Always return clean JSON only. Do not hallucinate.
                        If you are unsure about a field, leave it as an empty string.
                        Extract the following fields from the invoice: invoice_number, date, company_name, company_tax_number, seller, total_amount.
                        - Return the output as a valid JSON object only, no markdown or code blocks.
                        - Use double quotes (") for all keys and string values.
                        - Format invoice_number as a string to preserve leading zeroes.
                        - The date must be formatted as yyyy-mm-dd and usually >= 2024.
                        - company_name should be the name of the buyer company (the one purchasing the product) — not the seller.
                        - company_tax_number as a string to preserve leading zeroes.
                        - seller is the name of the company (the one selling the product).
                        - total_amount should be a number without any commas, currency symbols, or formatting.

                    Here is the invoice text:
                    {text}
                    """,
                    }
                ],
                max_tokens=500,
            )

            result_str = response.choices[0].message.content.strip()
            # Clean and parse
            result_str = result_str.strip("`")
            result_str = re.sub(r"^```(?:json|python)?", "", result_str).strip()
            result_str = re.sub(r"```$", "", result_str).strip()
            if result_str.count("{") > result_str.count("}"):
                result_str += "}"
            if result_str.startswith("{'") or result_str.startswith("['"):
                result_str = result_str.replace("'", '"')

            try:
                result_dict = json.loads(result_str)
            except json.JSONDecodeError:
                result_dict = ast.literal_eval(result_str)

            if not isinstance(result_dict, dict):
                raise ValueError("Parsed result is not a dictionary")

            return result_dict

        except Exception as e:
            logger.error(f"Error parsing result: {e}")
            return {"error": "Failed to parse result"}

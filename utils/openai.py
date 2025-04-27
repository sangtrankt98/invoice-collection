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
import unicodedata
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="pdfplumber")
warnings.filterwarnings("ignore", category=UserWarning, module="pdfminer")
# Set up logger
logger = logging.getLogger("invoice_collection.invoice")

default_dict = {
    "document_type": None,
    "document_number": None,
    "date": None,
    "entity_name": None,
    "entity_tax_number": None,
    "counterparty_name": None,
    "counterparty_tax_number": None,
    "payment_method": None,
    "amount_before_tax": None,
    "tax_rate": None,
    "tax_amount": None,
    "total_amount": None,
    "direction": None,
    "description": None,
}


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

    def normalize_vietnamese_text(self, text):
        """
        Normalize company names by:
        1. Removing Vietnamese diacritical marks
        2. Standardizing case and spacing
        3. Removing special characters
        """
        if not isinstance(text, str):
            return text

        # Vietnamese character mappings
        vietnamese_map = {
            "à": "a",
            "á": "a",
            "ả": "a",
            "ã": "a",
            "ạ": "a",
            "ă": "a",
            "ằ": "a",
            "ắ": "a",
            "ẳ": "a",
            "ẵ": "a",
            "ặ": "a",
            "â": "a",
            "ầ": "a",
            "ấ": "a",
            "ẩ": "a",
            "ẫ": "a",
            "ậ": "a",
            "đ": "d",
            "è": "e",
            "é": "e",
            "ẻ": "e",
            "ẽ": "e",
            "ẹ": "e",
            "ê": "e",
            "ề": "e",
            "ế": "e",
            "ể": "e",
            "ễ": "e",
            "ệ": "e",
            "ì": "i",
            "í": "i",
            "ỉ": "i",
            "ĩ": "i",
            "ị": "i",
            "ò": "o",
            "ó": "o",
            "ỏ": "o",
            "õ": "o",
            "ọ": "o",
            "ô": "o",
            "ồ": "o",
            "ố": "o",
            "ổ": "o",
            "ỗ": "o",
            "ộ": "o",
            "ơ": "o",
            "ờ": "o",
            "ớ": "o",
            "ở": "o",
            "ỡ": "o",
            "ợ": "o",
            "ù": "u",
            "ú": "u",
            "ủ": "u",
            "ũ": "u",
            "ụ": "u",
            "ư": "u",
            "ừ": "u",
            "ứ": "u",
            "ử": "u",
            "ữ": "u",
            "ự": "u",
            "ỳ": "y",
            "ý": "y",
            "ỷ": "y",
            "ỹ": "y",
            "ỵ": "y",
        }

        # Step 1: Remove Vietnamese diacritical marks
        result = ""
        for char in text:
            lower_char = char.lower()
            if lower_char in vietnamese_map:
                # Preserve the original case
                if char.isupper():
                    result += vietnamese_map[lower_char].upper()
                else:
                    result += vietnamese_map[lower_char]
            else:
                result += char

        # Step 2: Remove special characters and punctuation (keep alphanumeric and spaces)
        result = re.sub(r"[^\w\s]", "", result)

        # Step 3: Convert to uppercase and standardize spacing
        result = " ".join(result.upper().split())

        return result

    def extract_data_from_image(self, image_input):
        """
        Extract invoice data from an image using OpenAI API.

        Args:
            image_path (str): Path to the image file

        Returns:
            dict: Extracted invoice data
        """
        # Determine if the input is a file path or base64 string
        if os.path.exists(image_input) and not image_input.startswith("data:"):
            # It's a file path, read and encode to base64
            with open(image_input, "rb") as f:
                image_data = base64.b64encode(f.read()).decode("utf-8")
        else:
            # Assume it's already a base64 string
            image_data = image_input

        # Make sure image_data doesn't include the data URL prefix
        if image_data.startswith("data:image"):
            # Extract just the base64 part if it includes the data URL prefix
            image_data = image_data.split(",", 1)[1]

        response = openai.chat.completions.create(
            model="gpt-4.1",
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": """
                        You extract structured financial data from Vietnamese and English documents. Identify document type and extract relevant fields.
                        CLASSIFY as one of: INVOICE, BANK_TRANSACTION, TAX_DOCUMENT, OTHER_FINANCIAL

                        EXTRACT data into this JSON schema:
                            {{
                            "document_type": "",      // Type from above
                            "document_number": "",    // ID number (prefer 'Số'/'Number'/'No' over 'Ký Hiệu'/'Sign'/'Mã CQT'/Mã). It is invoice number when type = INVOICE NUMBER
                            "date": "",               // Format: yyyy-mm-dd
                            "entity_name": "",        // Main company/person
                            "entity_tax_number": "",  // Their tax ID
                            "counterparty_name": "",  // Other transaction party
                            "counterparty_tax_number": "", // Their tax ID
                            "payment_method": "",     // "bank_transfer", "cash", or "others"
                            "amount_before_tax": 0,   // Number integer only, not comma not dot
                            "tax_rate": 0,            // Number only (10 for 10%)
                            "tax_amount": 0,          // Number integer only, not comma not dot
                            "total_amount": 0,        // Number integer only, not comma not dot
                            "description": ""         // Brief context (50-60 chars), for invoices only
                            }}

                        - Keep identifier fields as strings, monetary values as numbers without formatting
                        - Use empty values for missing fields ("" for strings, 0 for numbers)
                        - For TAX_DOCUMENTS: entity_name = taxpayer
                        - For BANK_TRANSACTIONS: entity_name = account holder
                        Here is the document text:
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
            max_tokens=1000,
        )

        # Parse the response to get a dictionary
        result_str = response.choices[0].message.content
        result_dict = default_dict.copy()
        try:
            try:
                extracted_data = json.loads(result_str)
            except json.JSONDecodeError:
                extracted_data = ast.literal_eval(result_str)

                # Convert empty strings to None and keep numeric zeros as is
            # Convert empty strings to None, normalize Vietnamese text, and keep numeric zeros as is
            for key, value in extracted_data.items():
                if key in result_dict:
                    if isinstance(value, str):
                        if value.strip() == "":
                            result_dict[key] = None
                        else:
                            # Normalize Vietnamese text fields
                            if key in [
                                "entity_name",
                                "counterparty_name",
                            ]:
                                result_dict[key] = self.normalize_vietnamese_text(value)
                            else:
                                result_dict[key] = value
                    elif value == 0 and key in [
                        "amount_before_tax",
                        "tax_rate",
                        "tax_amount",
                        "total_amount",
                    ]:
                        # Keep numeric zeros as they are, could mean actual zero or missing
                        result_dict[key] = value
                    else:
                        result_dict[key] = value

            return result_dict
        except Exception as e:
            logger.error(f"Error parsing result: {e}")
            logger.error(f"Original result: {result_str}")
            return result_dict

    def extract_data_from_pdf_text(self, pdf_path):
        """
        Extract invoice data from a PDF using pdfplumber + OpenAI API (text-based approach).

        Args:
            pdf_path (str): Path to the PDF file

        Returns:
            dict: Extracted invoice data
        """
        result_dict = default_dict.copy()
        # Extract text from PDF
        with pdfplumber.open(pdf_path) as pdf:
            text = "\n".join(page.extract_text() or "" for page in pdf.pages)

        if not text.strip():
            raise ValueError("No text extracted from PDF")

        response = openai.chat.completions.create(
            model="gpt-4-turbo",
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "user",
                    "content": f"""You extract structured financial data from Vietnamese and English documents. Identify document type and extract relevant fields.
                CLASSIFY as one of: INVOICE, BANK_TRANSACTION, TAX_DOCUMENT, OTHER_FINANCIAL

                EXTRACT data into this JSON schema:
                    {{
                    "document_type": "",      // Type from above
                    "document_number": "",    // ID number (prefer 'Số'/'Number'/'No' over 'Ký Hiệu'/'Sign'/'Mã CQT'/Mã). It is invoice number when type = INVOICE NUMBER
                    "date": "",               // Format: yyyy-mm-dd
                    "entity_name": "",        // Main company/person
                    "entity_tax_number": "",  // Their tax ID
                    "counterparty_name": "",  // Other transaction party
                    "counterparty_tax_number": "", // Their tax ID
                    "payment_method": "",     // "bank_transfer", "cash", or "others"
                    "amount_before_tax": 0,   // Number integer only, not comma not dot
                    "tax_rate": 0,            // Number only (10 for 10%)
                    "tax_amount": 0,          // Number integer only, not comma not dot
                    "total_amount": 0,        // Number integer only, not comma not dot
                    "description": ""         // Brief context (50-60 chars), for invoices only
                    }}

                - Keep identifier fields as strings, monetary values as numbers without formatting
                - Use empty values for missing fields ("" for strings, 0 for numbers)
                - For TAX_DOCUMENTS: entity_name = taxpayer
                - For BANK_TRANSACTIONS: entity_name = account holder
                Here is the document text:
                {text}""",
                }
            ],
            max_tokens=1000,
        )

        # Parse the response to get a dictionary
        result_str = response.choices[0].message.content
        result_dict = default_dict.copy()
        try:
            try:
                extracted_data = json.loads(result_str)
            except json.JSONDecodeError:
                extracted_data = ast.literal_eval(result_str)

            # Convert empty strings to None, normalize Vietnamese text, and keep numeric zeros as is
            for key, value in extracted_data.items():
                if key in result_dict:
                    if isinstance(value, str):
                        if value.strip() == "":
                            result_dict[key] = None
                        else:
                            # Normalize Vietnamese text fields
                            if key in [
                                "entity_name",
                                "counterparty_name",
                            ]:
                                result_dict[key] = self.normalize_vietnamese_text(value)
                            else:
                                result_dict[key] = value
                    elif value == 0 and key in [
                        "amount_before_tax",
                        "tax_rate",
                        "tax_amount",
                        "total_amount",
                    ]:
                        # Keep numeric zeros as they are, could mean actual zero or missing
                        result_dict[key] = value
                    else:
                        result_dict[key] = value

            return result_dict
        except Exception as e:
            logger.error(f"Error parsing result: {e}")
            logger.error(f"Original result: {result_str}")
            return result_dict

    def extract_data_from_text(self, text):
        """
        Extract invoice data from a PDF using pdfplumber + OpenAI API (text-based approach).

        Args:
            pdf_path (str): Path to the PDF file

        Returns:
            dict: Extracted invoice data
        """
        result_dict = default_dict.copy()
        response = openai.chat.completions.create(
            model="gpt-4-turbo",
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "user",
                    "content": f"""You extract structured financial data from Vietnamese and English documents. Identify document type and extract relevant fields.
                CLASSIFY as one of: INVOICE, BANK_TRANSACTION, TAX_DOCUMENT, OTHER_FINANCIAL

                EXTRACT data into this JSON schema:
                    {{
                    "document_type": "",      // Type from above
                    "document_number": "",    // ID number (prefer 'Số'/'Number'/'No' over 'Ký Hiệu'/'Sign'/'Mã CQT'/Mã). It is invoice number when type = INVOICE NUMBER
                    "date": "",               // Format: yyyy-mm-dd
                    "entity_name": "",        // Main company/person
                    "entity_tax_number": "",  // Their tax ID
                    "counterparty_name": "",  // Other transaction party
                    "counterparty_tax_number": "", // Their tax ID
                    "payment_method": "",     // "bank_transfer", "cash", or "others"
                    "amount_before_tax": 0,   // Number integer only, not comma not dot
                    "tax_rate": 0,            // Number only (10 for 10%)
                    "tax_amount": 0,          // Number integer only, not comma not dot
                    "total_amount": 0,        // Number integer only, not comma not dot
                    "description": ""         // Brief context (50-60 chars), for invoices only
                    }}

                - Keep identifier fields as strings, monetary values as numbers without formatting
                - Use empty values for missing fields ("" for strings, 0 for numbers)
                - For TAX_DOCUMENTS: entity_name = taxpayer
                - For BANK_TRANSACTIONS: entity_name = account holder
                Here is the document text:
                {text}""",
                }
            ],
            max_tokens=1000,
        )

        # Parse the response to get a dictionary
        result_str = response.choices[0].message.content
        result_dict = default_dict.copy()
        try:
            try:
                extracted_data = json.loads(result_str)
            except json.JSONDecodeError:
                extracted_data = ast.literal_eval(result_str)
            # Convert empty strings to None, normalize Vietnamese text, and keep numeric zeros as is
            for key, value in extracted_data.items():
                if key in result_dict:
                    if isinstance(value, str):
                        if value.strip() == "":
                            result_dict[key] = None
                        else:
                            # Normalize Vietnamese text fields
                            if key in [
                                "entity_name",
                                "counterparty_name",
                            ]:
                                result_dict[key] = self.normalize_vietnamese_text(value)
                            else:
                                result_dict[key] = value
                    elif value == 0 and key in [
                        "amount_before_tax",
                        "tax_rate",
                        "tax_amount",
                        "total_amount",
                    ]:
                        # Keep numeric zeros as they are, could mean actual zero or missing
                        result_dict[key] = value
                    else:
                        result_dict[key] = value

            return result_dict
        except Exception as e:
            logger.error(f"Error parsing result: {e}")
            logger.error(f"Original result: {result_str}")
            return result_dict

    def extract_data_from_pdf(self, pdf_path):
        """
        Extract invoice data from a PDF, handling multi-page documents with various extraction methods.

        Args:
            pdf_path (str): Path to the PDF file

        Returns:
            dict: Extracted invoice data
        """
        result_dict = default_dict.copy()

        # First try to extract text using pdfplumber
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages)

            # If we got text, use the text-based extraction
            if text.strip():
                logger.info(
                    f"Successfully extracted text from PDF, using text-based extraction"
                )
                return self.extract_data_from_pdf_text(pdf_path)
        except Exception as e:
            logger.warning(f"Error extracting text with pdfplumber: {e}")
            text = ""

        # If no text was extracted or an error occurred, fall back to image-based extraction
        if not text.strip():
            logger.info(
                f"No text extracted from PDF, falling back to image-based extraction"
            )

            # Try different methods to convert PDF to image
            try:
                # Get the best image representation of the PDF that we can
                image_data = self._get_pdf_as_image(pdf_path)
                if image_data:
                    # Use the image-based extraction
                    return self.extract_data_from_image(image_data)
                else:
                    logger.error("Could not convert PDF to image")
                    return result_dict
            except Exception as e:
                logger.error(f"Error in image-based extraction: {e}")
                return result_dict

        return result_dict

    def _get_pdf_as_image(self, pdf_path):
        """
        Convert a PDF to an image for processing.
        Handles multiple pages by concatenating them vertically.

        Args:
            pdf_path (str): Path to the PDF file

        Returns:
            str: Base64-encoded image data or None if conversion failed
        """
        # Try pdf2image first (requires poppler)
        try:
            from pdf2image import convert_from_path
            import io
            import base64
            from PIL import Image

            # Specify Poppler path if needed
            poppler_path = None  # Set this to your Poppler path if needed

            # Convert all pages of the PDF to images
            images = (
                convert_from_path(pdf_path, poppler_path=poppler_path)
                if poppler_path
                else convert_from_path(pdf_path)
            )

            if not images:
                logger.warning("No images extracted from PDF using pdf2image")
                return None

            # If there's only one page, use it directly
            if len(images) == 1:
                img = images[0]
                img_byte_arr = io.BytesIO()
                img.save(img_byte_arr, format="JPEG", quality=85)
                img_byte_arr = img_byte_arr.getvalue()
                return base64.b64encode(img_byte_arr).decode("utf-8")

            # If there are multiple pages, concatenate them vertically
            # First determine total height and max width
            total_height = sum(img.height for img in images)
            max_width = max(img.width for img in images)

            # Create a new image with the combined dimensions
            combined_img = Image.new("RGB", (max_width, total_height))

            # Paste each page image into the combined image
            y_offset = 0
            for img in images:
                combined_img.paste(img, (0, y_offset))
                y_offset += img.height

            # Convert the combined image to bytes
            img_byte_arr = io.BytesIO()
            combined_img.save(img_byte_arr, format="JPEG", quality=85)
            img_byte_arr = img_byte_arr.getvalue()

            # Convert to base64
            return base64.b64encode(img_byte_arr).decode("utf-8")

        except Exception as e:
            logger.warning(f"pdf2image conversion failed: {e}")

        # Try PyMuPDF as a fallback
        try:
            import fitz  # PyMuPDF
            import io
            import base64
            from PIL import Image

            doc = fitz.open(pdf_path)

            if doc.page_count == 0:
                logger.warning("PDF has no pages according to PyMuPDF")
                return None

            # If there's only one page
            if doc.page_count == 1:
                page = doc.load_page(0)
                pix = page.get_pixmap(
                    matrix=fitz.Matrix(2, 2)
                )  # 2x scaling for better quality

                img_data = pix.samples
                img = Image.frombytes("RGB", [pix.width, pix.height], img_data)

                img_byte_arr = io.BytesIO()
                img.save(img_byte_arr, format="JPEG", quality=85)
                img_byte_arr = img_byte_arr.getvalue()

                return base64.b64encode(img_byte_arr).decode("utf-8")

            # If multiple pages, combine them
            else:
                # Create list to hold individual page images
                page_images = []
                total_height = 0
                max_width = 0

                # Convert each page to an image
                for page_num in range(doc.page_count):
                    page = doc.load_page(page_num)
                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))

                    img_data = pix.samples
                    img = Image.frombytes("RGB", [pix.width, pix.height], img_data)

                    page_images.append(img)
                    total_height += img.height
                    max_width = max(max_width, img.width)

                # Create a new image with the combined dimensions
                combined_img = Image.new("RGB", (max_width, total_height))

                # Paste each page image into the combined image
                y_offset = 0
                for img in page_images:
                    combined_img.paste(img, (0, y_offset))
                    y_offset += img.height

                # Convert the combined image to bytes
                img_byte_arr = io.BytesIO()
                combined_img.save(img_byte_arr, format="JPEG", quality=85)
                img_byte_arr = img_byte_arr.getvalue()

                # Convert to base64
                return base64.b64encode(img_byte_arr).decode("utf-8")

        except Exception as e:
            logger.warning(f"PyMuPDF conversion failed: {e}")

        # If all methods fail
        logger.error("All PDF to image conversion methods failed")
        return None

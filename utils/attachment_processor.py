"""
Module for processing email attachments
"""

import base64
import logging
import tempfile
import zipfile
import rarfile
import tarfile
import py7zr
import os
import re
import shutil
import uuid
import requests
from datetime import datetime
from urllib.parse import urlparse
from utils.llama import InvoiceExtractor
import warnings
import torch

warnings.filterwarnings("ignore", category=UserWarning, module="pdfminer")
logging.getLogger("pdfminer").setLevel(logging.ERROR)

from utils.logger_setup import setup_logger

logger = setup_logger()


class AttachmentProcessor:
    """Handles the processing of email attachments"""

    def __init__(self, gmail_service, drive_handler):
        """Initialize with Gmail service"""
        logger.info("Initializing attachment processor")
        self.gmail_service = gmail_service
        self.invoice_extractor = InvoiceExtractor(
            model_id="models/llama_321I",
            device_map="auto",
            torch_dtype=torch.bfloat16 if torch.cuda.is_available() else torch.float32,
            memory_threshold_gb=0.9,  # Clean up when memory usage exceeds 70%
            batch_size=5,  # Process 5 documents before forced cleanup
            output_dir="extraction_results",
        )
        self.drive_handler = drive_handler
        # Add HTTP session for downloading files from links
        self.session = requests.Session()
        # Configure timeout and headers
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
        )

    def get_attachments(self, user_id, msg_id, message):
        """Get and process attachments from the message"""
        logger.debug(f"Processing attachments for message ID: {msg_id}")
        attachments = []

        def process_parts(parts):
            for part in parts:
                if "parts" in part:
                    process_parts(part["parts"])

                if "filename" in part and part["filename"]:
                    if "body" in part and "attachmentId" in part["body"]:
                        try:
                            attachment = (
                                self.gmail_service.users()
                                .messages()
                                .attachments()
                                .get(
                                    userId=user_id,
                                    messageId=msg_id,
                                    id=part["body"]["attachmentId"],
                                )
                                .execute()
                            )

                            file_data = base64.urlsafe_b64decode(attachment["data"])
                            file_name = part["filename"]
                            mime_type = part["mimeType"]

                            logger.info(
                                f"Found attachment: {file_name}, type: {mime_type}, size: {len(file_data)} bytes"
                            )

                            attachments.append(
                                {
                                    "filename": file_name,
                                    "data": file_data,
                                    "size": len(file_data),
                                    "mime_type": mime_type,
                                    "gdrive_link": None,
                                }
                            )
                        except Exception as e:
                            logger.error(
                                f"Error processing attachment {part.get('filename', 'unknown')}: {e}"
                            )

        # Helper function to extract email body (text or HTML)
        def get_email_body_text(parts):
            for part in parts:
                if "parts" in part:
                    result = get_email_body_text(part["parts"])
                    if result:
                        return result

                if part.get("mimeType") in ["text/plain", "text/html"]:
                    body_data = part.get("body", {}).get("data")
                    if body_data:
                        try:
                            return base64.urlsafe_b64decode(body_data).decode("utf-8")
                        except Exception as e:
                            logger.warning(f"Failed to decode email body part: {e}")
            return None

        # Process file attachments
        if "parts" in message["payload"]:
            process_parts(message["payload"]["parts"])

        # Get the email body
        decoded_body = get_email_body_text([message["payload"]])
        if decoded_body:
            # Process Google Drive folder links
            folder_ids = self.drive_handler.extract_drive_folder_ids(decoded_body)
            for folder_id in folder_ids:
                files = self.drive_handler.list_files_in_folder(folder_id)
                for f in files:
                    file_id = f["id"]
                    metadata = self.drive_handler.get_file_metadata(file_id)
                    file_data = self.drive_handler.download_file(file_id)
                    if metadata and file_data:
                        attachments.append(
                            {
                                "filename": metadata.get("name"),
                                "data": file_data,
                                "size": int(metadata.get("size", 0)),
                                "mime_type": metadata.get(
                                    "mimeType", "application/octet-stream"
                                ),
                                "gdrive_link": f"https://drive.google.com/file/d/{file_id}",
                            }
                        )
                logger.info(f"Found Google Drive folder link: {folder_id}")

            # Process e-invoice download links
            invoice_links = self.extract_invoice_links(decoded_body)
            for link_info in invoice_links:
                try:
                    file_data = self.download_from_link(link_info["url"])
                    if file_data:
                        # Generate filename from URL if none provided
                        filename = link_info.get(
                            "filename"
                        ) or self.generate_filename_from_url(link_info["url"])

                        # Try to determine MIME type from content or default to PDF for invoices
                        mime_type = (
                            self.determine_mime_type(file_data) or "application/pdf"
                        )

                        attachments.append(
                            {
                                "filename": filename,
                                "data": file_data,
                                "size": len(file_data),
                                "mime_type": mime_type,
                                "gdrive_link": None,
                                "source_link": link_info["url"],
                            }
                        )
                        logger.info(f"Downloaded invoice from link: {link_info['url']}")
                    else:
                        # Just log and continue if download fails
                        logger.warning(
                            f"Skipping inaccessible invoice link: {link_info['url']}"
                        )
                except Exception as e:
                    # Log error but don't fail the whole process
                    logger.warning(f"Skipping invoice link {link_info['url']}: {e}")

        logger.debug(
            f"Total {len(attachments)} attachments (files + gdrive + invoice links) in message ID: {msg_id}"
        )
        return attachments

    def extract_invoice_links(self, body_text):
        """Extract e-invoice download links from email body"""
        invoice_links = []
        seen_urls = set()  # To track and eliminate duplicate URLs

        # Match common e-invoice domains and download paths
        invoice_patterns = [
            # Pattern for easyinvoice.vn links like in the example
            r'(https?://[^\s]*?easyinvoice\.vn/Invoice/(?:DownloadInv(?:Pdf|oice)|Download)[^\s\'"]*)',
            # Additional patterns for other e-invoice providers in Vietnam
            r'(https?://[^\s]*?einvoice[^\s/]*?\.vn/[^\s\'"]*?(?:download|pdf)[^\s\'"]*)',
            r'(https?://[^\s]*?hoadon[^\s/]*?\.vn/[^\s\'"]*?(?:download|pdf)[^\s\'"]*)',
            r'(https?://[^\s]*?minvoice[^\s/]*?\.vn/[^\s\'"]*?(?:download|pdf)[^\s\'"]*)',
            r'(https?://[^\s]*?inv[^\s/]*?\.vn/[^\s\'"]*?(?:download|pdf)[^\s\'"]*)',
            # Generic patterns for invoice download links
            r'(https?://[^\s]*?[Ii]nvoice[^\s/]*?/[^\s\'"]*?(?:download|pdf)[^\s\'"]*)',
            r'(https?://[^\s]*?/[Ii]nvoice[^\s/]*/[^\s\'"]*?(?:token|key|id)[^\s\'"]*)',
        ]

        for pattern in invoice_patterns:
            matches = re.finditer(pattern, body_text, re.IGNORECASE)
            for match in matches:
                url = match.group(1)
                # Clean the URL (remove trailing punctuation, quotes, etc.)
                url = self.clean_url(url)

                # Skip if we've already seen this URL (remove duplicates)
                if url in seen_urls:
                    continue

                seen_urls.add(url)
                invoice_links.append(
                    {
                        "url": url,
                        "filename": None,  # We'll generate this later based on the URL or response
                    }
                )

        logger.info(f"Found {len(invoice_links)} unique invoice download links")
        return invoice_links

    def clean_url(self, url):
        """Clean up URL by removing trailing punctuation and quotes"""
        # Remove trailing punctuation that might be part of the message, not the URL
        url = re.sub(r'[.,;:\'"\)\]>]+$', "", url)
        # If URL is enclosed in parentheses, quotes, or brackets, remove them
        url = re.sub(r"^[\(\[\{\'\"<]|[\)\]\}\'\"<]$", "", url)
        return url

    def download_from_link(self, url):
        """
        Download file from a given URL using multiple fallback methods
        Tries several approaches in sequence until one succeeds
        """
        logger.info(f"Attempting to download from URL: {url}")

        # Define headers for requests
        headers = {
            "Referer": urlparse(url).scheme + "://" + urlparse(url).netloc + "/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
            "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Upgrade-Insecure-Requests": "1",
            "Connection": "keep-alive",
        }

        # Method 1: Try HTTPS if URL is HTTP (except localhost)
        if url.startswith("http:") and not url.startswith("http://localhost"):
            try:
                https_url = "https:" + url[5:]
                logger.info(f"Method 1: Trying with HTTPS instead of HTTP: {https_url}")
                response = self.session.get(
                    https_url, timeout=30, allow_redirects=True, headers=headers
                )
                if response.status_code == 200 and response.content.startswith(b"%PDF"):
                    logger.info(
                        f"Successfully downloaded PDF using HTTPS ({len(response.content)} bytes)"
                    )
                    return response.content
            except Exception as e:
                logger.warning(f"HTTPS attempt failed: {str(e)}")

        # Method 2: Try direct download with headers
        try:
            logger.info("Method 2: Attempting direct download with custom headers")
            response = self.session.get(
                url, timeout=30, allow_redirects=True, headers=headers
            )

            if response.status_code == 200 and response.content.startswith(b"%PDF"):
                logger.info(
                    f"Successfully downloaded PDF directly ({len(response.content)} bytes)"
                )
                return response.content
        except Exception as e:
            logger.warning(f"Direct download failed: {str(e)}")

        # Method 3: Try with Selenium for browser-based downloads
        try:
            logger.info("Method 3: Attempting download with Selenium")
            # Create temporary directory for download
            import tempfile

            temp_dir = tempfile.mkdtemp()

            try:
                file_data, _ = self.download_with_selenium(url, temp_dir)
                if file_data and file_data.startswith(b"%PDF"):
                    logger.info(
                        f"Successfully downloaded PDF with Selenium ({len(file_data)} bytes)"
                    )
                    return file_data
            finally:
                # Clean up temp directory
                import shutil

                shutil.rmtree(temp_dir, ignore_errors=True)
        except ImportError:
            logger.warning("Selenium not available for browser-based downloads")
        except Exception as e:
            logger.warning(f"Selenium download failed: {str(e)}")

        # Method 4: Try simple GET request without fancy headers
        try:
            logger.info("Method 4: Attempting simple GET request")
            response = self.session.get(url, timeout=30, allow_redirects=True)
            if response.status_code == 200:
                if response.content.startswith(b"%PDF"):
                    logger.info(
                        f"Successfully downloaded PDF with simple GET ({len(response.content)} bytes)"
                    )
                    return response.content
                else:
                    logger.warning("Response received but not a PDF file")
        except Exception as e:
            logger.warning(f"Simple GET request failed: {str(e)}")

        # If we've tried everything and still don't have a PDF, give up
        logger.warning(f"All download methods failed for URL: {url}")
        return None

    def generate_filename_from_url(self, url):
        """Generate a sensible filename from URL"""
        # Try to extract filename from URL
        path = urlparse(url).path
        filename = os.path.basename(path)

        # Handle special case for easyinvoice.vn
        if "easyinvoice.vn" in url:
            # Extract token from URL if present
            token_match = re.search(r"token=([^&]+)", url)
            if token_match:
                token = token_match.group(1)
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                # Create more meaningful filename
                return f"invoice_easyinvoice_{timestamp}_{token[:10]}.pdf"
            else:
                # If no token, create a generic filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                return f"invoice_easyinvoice_{timestamp}.pdf"

        # If no filename could be determined or it's too generic
        if not filename or filename in (
            "",
            "download",
            "view",
            "pdf",
            "DownloadInvPdf",
        ):
            # Create a filename using invoice and timestamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

            # Try to include domain in filename for better identification
            domain = urlparse(url).netloc
            domain = domain.replace("www.", "").split(".")[
                0
            ]  # Extract first part of domain

            filename = f"invoice_{domain}_{timestamp}.pdf"

        # Ensure filename has .pdf extension for invoices if no extension
        if not os.path.splitext(filename)[1]:
            filename += ".pdf"

        return filename

    def determine_mime_type(self, data):
        """Try to determine MIME type from file content"""
        try:
            import magic

            return magic.from_buffer(data, mime=True)
        except ImportError:
            # If python-magic is not available, try to guess from content
            if data.startswith(b"%PDF"):
                return "application/pdf"
            elif data.startswith(b"PK\x03\x04"):
                return "application/zip"  # Could be XLSX, DOCX, etc.
            # Add more signature checks as needed

            # Default to None, caller should decide on default
            return None

    def save_attachment_to_file(self, attachment, output_dir="downloads"):
        """Save attachment data to local file system"""
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, attachment["filename"])

        try:
            with open(filepath, "wb") as f:
                f.write(attachment["data"])
            logger.info(f"Saved attachment to: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save attachment {attachment['filename']}: {e}")
            return None

    def extract_archive(
        self, archive_path, output_dir="downloads", processed_archives=None
    ):
        """
        Recursively extracts archive files (.zip, .rar, .7z, .tar, .gz) and flattens all files into output_dir
        Will also extract nested archives within archives
        """
        if processed_archives is None:
            processed_archives = set()

        # Avoid processing the same archive twice (prevents infinite loops)
        if archive_path in processed_archives:
            return []

        processed_archives.add(archive_path)
        extracted_files = []
        temp_dir = tempfile.mkdtemp()

        try:
            os.makedirs(output_dir, exist_ok=True)

            # Extract the archive based on its extension
            if archive_path.endswith(".zip"):
                with zipfile.ZipFile(archive_path, "r") as zip_ref:
                    zip_ref.extractall(temp_dir)

            elif archive_path.endswith(".rar"):
                with rarfile.RarFile(archive_path, "r") as rar_ref:
                    rar_ref.extractall(temp_dir)

            elif archive_path.endswith(".7z"):
                with py7zr.SevenZipFile(archive_path, mode="r") as z:
                    z.extractall(path=temp_dir)

            elif archive_path.endswith(".tar") or archive_path.endswith(".gz"):
                with tarfile.open(archive_path, "r:*") as tar_ref:
                    tar_ref.extractall(path=temp_dir)

            else:
                logger.warning(f"Unsupported archive type: {archive_path}")
                shutil.rmtree(temp_dir, ignore_errors=True)
                return []

            # Process all extracted items
            for root, dirs, files in os.walk(temp_dir):
                # First, check for nested archives and extract them
                for file in files[
                    :
                ]:  # Create a copy of the list to modify during iteration
                    file_path = os.path.join(root, file)
                    # Check if file is an archive
                    if any(
                        file.endswith(ext)
                        for ext in [".zip", ".rar", ".7z", ".tar", ".gz"]
                    ):
                        # Recursively extract this nested archive
                        nested_files = self.extract_archive(
                            file_path,
                            output_dir=output_dir,
                            processed_archives=processed_archives,
                        )
                        extracted_files.extend(nested_files)
                        # Remove this file from the list as it's been processed as an archive
                        files.remove(file)

                # Now move the remaining non-archive files to the output directory
                for file in files:
                    src = os.path.join(root, file)
                    dst = os.path.join(output_dir, file)

                    # Handle filename collisions
                    if os.path.exists(dst):
                        base, ext = os.path.splitext(file)
                        dst = os.path.join(
                            output_dir, f"{base}_{uuid.uuid4().hex[:8]}{ext}"
                        )

                    shutil.move(src, dst)
                    extracted_files.append(dst)

            logger.info(
                f"Extracted {len(extracted_files)} files from {archive_path} to {output_dir}"
            )

        except Exception as e:
            logger.error(f"Error extracting archive {archive_path}: {e}")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        return extracted_files

    def process_pdf(self, attachment, pdf_path):
        """Process PDF attachment to extract invoice data"""
        logger.info(f"Processing PDF: {attachment['filename']}")
        data = self.invoice_extractor.extract_data_from_pdf(pdf_path)
        return {
            "document_type": data["document_type"],
            "document_number": data["document_number"],
            "date": data["date"],
            "entity_name": data["entity_name"],
            "entity_tax_number": data["entity_tax_number"],
            "counterparty_name": data["counterparty_name"],
            "counterparty_tax_number": data["counterparty_tax_number"],
            "payment_method": data["payment_method"],
            "amount_before_tax": data["amount_before_tax"],
            "tax_rate": data["tax_rate"],
            "tax_amount": data["tax_amount"],
            "total_amount": data["total_amount"],
            "direction": data["direction"],
            "description": data["description"],
        }

    # def process_image(self, attachment, img_path):
    #     """Process image attachment to extract invoice data"""
    #     logger.info(f"Processing image: {attachment['filename']}")
    #     data = self.invoice_extractor.extract_data_from_image(img_path)
    #     return {
    #         "document_type": data["document_type"],
    #         "document_number": data["document_number"],
    #         "date": data["date"],
    #         "entity_name": data["entity_name"],
    #         "entity_tax_number": data["entity_tax_number"],
    #         "counterparty_name": data["counterparty_name"],
    #         "counterparty_tax_number": data["counterparty_tax_number"],
    #         "payment_method": data["payment_method"],
    #         "amount_before_tax": data["amount_before_tax"],
    #         "tax_rate": data["tax_rate"],
    #         "tax_amount": data["tax_amount"],
    #         "total_amount": data["total_amount"],
    #         "direction": data["direction"],
    #         "description": data["description"],
    #     }

    def process_xml(self, attachment, xml_path):
        """Process XML files to extract invoice information"""
        logger.info(f"Processing XML: {attachment['filename']}")
        # Parse XML file
        import xml.etree.ElementTree as ET

        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Extract text representation from XML
        def element_to_text(elem, level=0):
            result = f"{' ' * level}{elem.tag}: {elem.text.strip() if elem.text and elem.text.strip() else ''}\n"
            for child in elem:
                result += element_to_text(child, level + 2)
            return result

        text = element_to_text(root)

        # Use the same AI-based extraction as for PDFs
        data = self.invoice_extractor.extract_data_from_text(text)
        return {
            "document_type": data["document_type"],
            "document_number": data["document_number"],
            "date": data["date"],
            "entity_name": data["entity_name"],
            "entity_tax_number": data["entity_tax_number"],
            "counterparty_name": data["counterparty_name"],
            "counterparty_tax_number": data["counterparty_tax_number"],
            "payment_method": data["payment_method"],
            "amount_before_tax": data["amount_before_tax"],
            "tax_rate": data["tax_rate"],
            "tax_amount": data["tax_amount"],
            "total_amount": data["total_amount"],
            "direction": data["direction"],
            "description": data["description"],
        }

    def download_with_selenium(self, url, download_dir):
        """
        Download file from URL using Selenium browser automation.
        Handles JavaScript-triggered downloads and complex authentication flows.

        Args:
            url: URL to download from
            download_dir: Directory to save downloaded files to

        Returns:
            Tuple of (file_data, file_name) or (None, None) if download fails
        """
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            import time
            import glob

            # Configure Chrome to download files to specific directory without prompting
            os.makedirs(download_dir, exist_ok=True)
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Run in headless mode
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")

            # Set download preferences
            prefs = {
                "download.default_directory": download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "plugins.always_open_pdf_externally": True,  # Force PDF downloads instead of opening in browser
            }
            chrome_options.add_experimental_option("prefs", prefs)

            # Get list of files in download directory before download
            before_files = set(os.listdir(download_dir))

            # Start browser and navigate to URL
            logger.info(f"Starting Selenium browser to download: {url}")
            service = Service()  # Use default chromedriver location
            driver = webdriver.Chrome(service=service, options=chrome_options)

            try:
                # Navigate to URL and wait for page to load
                driver.get(url)

                # Wait for download to complete (max 30 seconds)
                max_wait_time = 30
                download_complete = False
                start_time = time.time()

                while (
                    not download_complete and time.time() - start_time < max_wait_time
                ):
                    # Check for new files in download directory
                    current_files = set(os.listdir(download_dir))
                    new_files = current_files - before_files

                    # Check if any new files are being downloaded (look for .crdownload files)
                    downloading_files = [
                        f for f in new_files if f.endswith(".crdownload")
                    ]

                    if new_files and not downloading_files:
                        # Found new files and none are still downloading
                        download_complete = True
                    else:
                        # Wait a moment before checking again
                        time.sleep(1)

                # Get list of new files after download
                after_files = set(os.listdir(download_dir))
                new_files = after_files - before_files

                if new_files:
                    # Get the most recently modified file
                    newest_file = max(
                        [os.path.join(download_dir, f) for f in new_files],
                        key=os.path.getmtime,
                    )

                    file_name = os.path.basename(newest_file)

                    # Read file data
                    with open(newest_file, "rb") as f:
                        file_data = f.read()

                    logger.info(
                        f"Successfully downloaded file via Selenium: {file_name} ({len(file_data)} bytes)"
                    )
                    return file_data, file_name
                else:
                    logger.warning(f"No file was downloaded from {url} using Selenium")
                    return None, None

            finally:
                # Clean up
                driver.quit()

        except Exception as e:
            logger.error(f"Error using Selenium to download {url}: {e}")
            return None, None

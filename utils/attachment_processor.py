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
import shutil
import uuid
from utils.openai import InvoiceExtractor
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="pdfminer")
logging.getLogger("pdfminer").setLevel(logging.ERROR)

# Set up logger
logger = logging.getLogger("invoice_collection.attachments")


class AttachmentProcessor:
    """Handles the processing of email attachments"""

    def __init__(self, gmail_service, api_key, drive_handler):
        """Initialize with Gmail service"""
        logger.info("Initializing attachment processor")
        self.gmail_service = gmail_service
        self.invoice_extractor = InvoiceExtractor(api_key)
        self.drive_handler = drive_handler

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

        # Check for Google Drive folder links in email body
        decoded_body = get_email_body_text([message["payload"]])
        if decoded_body:
            folder_ids = self.drive_handler.extract_drive_folder_ids(decoded_body)
            for folder_id in folder_ids:
                # print(folder_id)
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
        logger.debug(
            f"Total {len(attachments)} attachments (files + gdrive) in message ID: {msg_id}"
        )
        return attachments

    def get_email_body_text(parts):
        for part in parts:
            if part.get("parts"):
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
        """Process PDF attachment - placeholder for future implementation"""
        logger.info(f"Processing PDF: {attachment['filename']}")
        data = self.invoice_extractor.extract_data_from_pdf_text(pdf_path)
        # logger.info(f"Data extract from PDF is {data}")
        return {
            "invoice_number": data["invoice_number"],
            "date": data["date"],
            "company_name": data["company_name"],
            "company_tax_number": data["company_tax_number"],
            "seller": data["seller"],
            "total_amount": data["total_amount"],
        }

    def process_image(self, attachment, img_path):
        """Process PDF attachment - placeholder for future implementation"""
        logger.info(f"Processing PDF: {attachment['filename']}")
        data = self.invoice_extractor.extract_data_from_image(img_path)
        # logger.info(f"Data extract from PDF is {data}")
        return {
            "invoice_number": data["invoice_number"],
            "date": data["date"],
            "company_name": data["company_name"],
            "company_tax_number": data["company_tax_number"],
            "seller": data["seller"],
            "total_amount": data["total_amount"],
        }

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
            "invoice_number": data["invoice_number"],
            "date": data["date"],
            "company_name": data["company_name"],
            "company_tax_number": data["company_tax_number"],
            "seller": data["seller"],
            "total_amount": data["total_amount"],
        }


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
import shutil
import uuid
from utils.openai import InvoiceExtractor
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="pdfminer")
logging.getLogger("pdfminer").setLevel(logging.ERROR)

# Set up logger
logger = logging.getLogger("invoice_collection.attachments")


class AttachmentProcessor:
    """Handles the processing of email attachments"""

    def __init__(self, gmail_service, api_key, drive_handler):
        """Initialize with Gmail service"""
        logger.info("Initializing attachment processor")
        self.gmail_service = gmail_service
        self.invoice_extractor = InvoiceExtractor(api_key)
        self.drive_handler = drive_handler

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

        # Check for Google Drive folder links in email body
        decoded_body = get_email_body_text([message["payload"]])
        if decoded_body:
            folder_ids = self.drive_handler.extract_drive_folder_ids(decoded_body)
            for folder_id in folder_ids:
                # print(folder_id)
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
        logger.debug(
            f"Total {len(attachments)} attachments (files + gdrive) in message ID: {msg_id}"
        )
        return attachments

    def get_email_body_text(parts):
        for part in parts:
            if part.get("parts"):
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
        """Process PDF attachment - placeholder for future implementation"""
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

    def process_image(self, attachment, img_path):
        """Process PDF attachment - placeholder for future implementation"""
        logger.info(f"Processing PDF: {attachment['filename']}")
        data = self.invoice_extractor.extract_data_from_image(img_path)
        # logger.info(f"Data extract from PDF is {data}")
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

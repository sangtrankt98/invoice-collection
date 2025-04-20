"""
Module for processing email attachments
"""

import base64
import logging
import os
import re
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

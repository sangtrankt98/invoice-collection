"""
Gmail handler module for fetching and processing emails
"""

import base64
import re
import os
from googleapiclient.errors import HttpError
from utils.auth import GoogleAuthenticator
from utils.attachment_processor import AttachmentProcessor
from utils.logger_setup import setup_logger

logger = setup_logger()


class GmailHandler:
    """Handles Gmail API operations"""

    def __init__(self, credentials, api_key, drive_handler):
        """Initialize with Google credentials"""
        logger.info("Initializing Gmail handler")
        self.service = GoogleAuthenticator.create_service("gmail", "v1", credentials)
        self.attachment_processor = AttachmentProcessor(
            self.service, api_key, drive_handler
        )

    def extract_email_content(
        self, user_id="me", query="has:attachment", max_results=10
    ):
        """Fetch emails and extract content"""
        logger.info(
            f"Extracting emails with query: '{query}', max results: {max_results}"
        )
        try:
            # Get messages that match the query
            results = (
                self.service.users()
                .messages()
                .list(userId=user_id, q=query, maxResults=max_results)
                .execute()
            )

            messages = results.get("messages", [])
            if not messages:
                logger.warning("No messages found matching the query")
                return []

            logger.info(f"Found {len(messages)} messages matching the query")
            email_data = []
            for i, message in enumerate(messages):
                logger.info(f"Processing message {i+1} of {len(messages)}")
                try:
                    msg = (
                        self.service.users()
                        .messages()
                        .get(userId=user_id, id=message["id"])
                        .execute()
                    )

                    # Process email
                    email_info = self._process_email(user_id, message["id"], msg)
                    email_data.append(email_info)
                    logger.debug(f"Successfully processed message ID: {message['id']}")
                except Exception as e:
                    logger.error(f"Error processing message ID {message['id']}: {e}")

            logger.info(
                f"Successfully processed {len(email_data)} out of {len(messages)} messages"
            )
            return email_data

        except HttpError as error:
            logger.error(f"Gmail API error: {error}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error extracting emails: {e}")
            return []

    def _process_email(self, user_id, message_id, message):
        """Process a single email message"""
        logger.debug(f"Processing email message: {message_id}")
        # Get email headers
        headers = message["payload"]["headers"]
        subject = next(
            (
                header["value"]
                for header in headers
                if header["name"].lower() == "subject"
            ),
            "No Subject",
        )
        from_email = next(
            (header["value"] for header in headers if header["name"].lower() == "from"),
            "No Sender",
        )
        date = next(
            (header["value"] for header in headers if header["name"].lower() == "date"),
            "No Date",
        )

        logger.debug(f"Email from: {from_email}, subject: {subject}")

        # Get email body
        body = self._get_email_body(message)

        # Extract attachments
        attachments = self.attachment_processor.get_attachments(
            user_id, message_id, message
        )
        processed_attachments = []
        # for att in attachments[:]:
        # # This makes a shallow copy of the list, so any changes to attachments during iteration won’t affect the loop.
        for att in attachments:
            local_path = self.attachment_processor.save_attachment_to_file(att)
            if local_path:
                # Archive handling (ZIP or RAR)
                if local_path.lower().endswith((".zip", ".rar", ".7z", ".tar", ".gz")):
                    extracted_files = self.attachment_processor.extract_archive(
                        local_path
                    )
                    for file_path in extracted_files:
                        processed_att = self._process_attachment_by_type(
                            att.copy(), file_path
                        )
                        processed_attachments.append(processed_att)
                else:
                    # Direct file handling
                    processed_att = self._process_attachment_by_type(att, local_path)
                    processed_attachments.append(processed_att)

        # Replace attachments list if needed
        attachments = processed_attachments

        if attachments:
            logger.info(f"Found {len(attachments)} attachments in the email")

        # Create a summary of the email (simple version)
        summary = f"From: {from_email}\nSubject: {subject}\nDate: {date}\n\nBody Summary: {body[:200]}..."

        return {
            "message_id": message_id,
            "date": date,
            "subject": subject,
            "from": from_email,
            "body": body,
            "summary": summary,
            "attachments": attachments,
        }

    @staticmethod
    def _get_email_body(message):
        """Extract the email body from the message"""
        body = ""

        if "parts" in message["payload"]:
            for part in message["payload"]["parts"]:
                if part["mimeType"] == "text/plain" and "data" in part["body"]:
                    body = base64.urlsafe_b64decode(part["body"]["data"]).decode(
                        "utf-8"
                    )
                    break
                elif part["mimeType"] == "text/html" and "data" in part["body"]:
                    # This is HTML content, you might want to use an HTML parser
                    html_body = base64.urlsafe_b64decode(part["body"]["data"]).decode(
                        "utf-8"
                    )
                    # Simple stripping of HTML tags for this example
                    body = re.sub("<[^<]+?>", "", html_body)
                    break
        elif "body" in message["payload"] and "data" in message["payload"]["body"]:
            body = base64.urlsafe_b64decode(message["payload"]["body"]["data"]).decode(
                "utf-8"
            )

        return body

    def _process_attachment_by_type(self, att, file_path):
        """Process an attachment based on its file type"""
        file_extension = os.path.splitext(file_path)[1].lower()
        att["file_name"] = os.path.basename(file_path)

        # Default values for DataFrame compatibility
        default_values = {
            "invoice_number": "",
            "date": "",
            "company_name": "",
            "company_tax_number": "",
            "seller": "",
            "total_amount": "",
            "file_type": (
                file_extension[1:] if file_extension.startswith(".") else "unknown"
            ),
            "processed": False,
            "skipped": False,
            "error": "",
        }

        # File type handlers
        try:
            if file_extension == ".pdf":
                return self._process_pdf_attachment(att, file_path)
            elif file_extension in (".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp"):
                return self._process_image_attachment(att, file_path)
            elif file_extension == ".xml":
                return self._process_xml_attachment(att, file_path)
            else:
                logger.info(f"Unsupported file type: {file_path}")
                att.update(default_values)
                att["skipped"] = True
                return att
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            att.update(default_values)
            att["error"] = str(e)
            return att

    def _process_pdf_attachment(self, att, file_path):
        """Process PDF attachments"""
        process_result = self.attachment_processor.process_pdf(att, file_path)
        return self._handle_processed_document(att, process_result, file_path)

    def _process_image_attachment(self, att, file_path):
        """Process image attachments (may contain scanned invoices)"""
        try:
            # Call a method from attachment processor to handle images (may use OCR)
            process_result = self.attachment_processor.process_image(att, file_path)
            return self._handle_processed_document(att, process_result, file_path)
        except Exception as e:
            logger.error(f"Error processing image {file_path}: {str(e)}")
            att["file_type"] = "image"
            att["error"] = str(e)
            att["processed"] = False
            return att

    def _process_xml_attachment(self, att, file_path):
        """Process XML attachments (may contain structured invoice data)"""
        try:
            # Call a method from attachment processor to handle XML files
            process_result = self.attachment_processor.process_xml(att, file_path)
            return self._handle_processed_document(att, process_result, file_path)
        except Exception as e:
            logger.error(f"Error processing XML file {file_path}: {str(e)}")
            att["file_type"] = "xml"
            att["error"] = str(e)
            att["processed"] = False
            return att

    def _handle_processed_document(self, att, process_result, file_path):
        """Handle any processed document with invoice data"""
        # Set default values to prevent KeyError
        default_values = {
            "invoice_number": "",
            "date": "",
            "company_name": "",
            "company_tax_number": "",
            "seller": "",
            "total_amount": "",
        }

        # Update with values from process_result, using defaults for missing keys
        for key, default_value in default_values.items():
            att[key] = process_result.get(key, default_value)

        # Set file type based on extension
        file_extension = os.path.splitext(file_path)[1].lower()
        att["file_type"] = file_extension[1:]  # Remove the dot
        att["processed"] = True

        # Rename file if invoice number is available
        if process_result.get("invoice_number"):
            new_filename = f"{process_result.get('date', 'unknown')}_{process_result['invoice_number']}{file_extension}"
            new_filename = new_filename.replace("/", "_").replace(":", "_")
            new_path = os.path.join(os.path.dirname(file_path), new_filename)
            att["file_name"] = new_filename

            # Rename file if new path doesn't exist
            if not os.path.exists(new_path):
                try:
                    os.replace(file_path, new_path)
                    logger.info(f"Renamed file to: {new_filename}")
                except Exception as e:
                    logger.error(f"Error renaming file: {str(e)}")
            else:
                logger.info(f"Skipped renaming: {new_path} already exists")
        else:
            att["file_name"] = os.path.basename(file_path)

        return att

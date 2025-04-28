"""
Gmail handler module for fetching and processing emails
"""

import base64
import re
import os
from datetime import datetime, timedelta
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
        self, user_id="me", query="has:attachment", max_results=10, time_filter=None
    ):
        """Fetch emails and extract content with time-based filtering"""
        logger.info(
            f"Extracting emails with query: '{query}', max results: {max_results}, time filter: {time_filter}"
        )

        try:
            # Calculate cutoff time for filtering if time_filter is provided
            cutoff_time = None
            if time_filter:
                unit = time_filter[-1]
                try:
                    value = int(time_filter[:-1])
                    now = datetime.now()

                    if unit == "m":
                        cutoff_time = now - timedelta(minutes=value)
                        logger.info(
                            f"Using cutoff time for {value} minutes: {cutoff_time}"
                        )
                    elif unit == "h":
                        cutoff_time = now - timedelta(hours=value)
                        logger.info(
                            f"Using cutoff time for {value} hours: {cutoff_time}"
                        )
                except ValueError:
                    # Invalid time filter format, ignore
                    logger.warning(f"Invalid time filter format: {time_filter}")

            # Instead of fetching messages, fetch threads first
            results = (
                self.service.users()
                .threads()
                .list(userId=user_id, q=query, maxResults=max_results)
                .execute()
            )

            threads = results.get("threads", [])
            if not threads:
                logger.warning("No threads found matching the query")
                return []

            logger.info(f"Found {len(threads)} threads matching the query")
            email_data = []

            for i, thread in enumerate(threads):
                logger.info(f"Processing thread {i+1} of {len(threads)}")
                try:
                    # Get the full thread with all messages
                    thread_data = (
                        self.service.users()
                        .threads()
                        .get(userId=user_id, id=thread["id"])
                        .execute()
                    )

                    # Process the thread with time filtering
                    result = self._process_thread(user_id, thread_data, cutoff_time)
                    if result:
                        email_data.append(result)

                except Exception as e:
                    logger.error(f"Error processing thread ID {thread['id']}: {e}")

            logger.info(
                f"Successfully processed {len(email_data)} out of {len(threads)} threads"
            )
            return email_data

        except HttpError as error:
            logger.error(f"Gmail API error: {error}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error extracting emails: {e}")
            return []

    def _process_thread(self, user_id, thread_data, cutoff_time=None):
        """Process a thread with time-based filtering of messages"""
        thread_id = thread_data["id"]
        messages = thread_data.get("messages", [])

        if not messages:
            logger.warning(f"Thread {thread_id} contains no messages")
            return None

        logger.info(f"Thread {thread_id} contains {len(messages)} messages")

        # If no cutoff time, use the most recent message
        if not cutoff_time:
            # Sort messages by internalDate in descending order (newest first)
            messages.sort(key=lambda msg: int(msg.get("internalDate", 0)), reverse=True)
            newest_message = messages[0]
            return self._process_single_message(user_id, newest_message)

        # Filter messages by cutoff time
        filtered_messages = []
        for message in messages:
            if "internalDate" in message:
                # Convert epoch milliseconds to datetime
                msg_timestamp = datetime.fromtimestamp(
                    int(message["internalDate"]) / 1000
                )

                if msg_timestamp >= cutoff_time:
                    logger.debug(
                        f"Including message with timestamp {msg_timestamp} >= cutoff {cutoff_time}"
                    )
                    filtered_messages.append(message)
                else:
                    logger.debug(
                        f"Excluding message with timestamp {msg_timestamp} < cutoff {cutoff_time}"
                    )

        if not filtered_messages:
            logger.info(f"No messages in thread {thread_id} are after cutoff time")
            return None

        logger.info(
            f"Thread {thread_id}: {len(filtered_messages)} of {len(messages)} messages passed time filter"
        )

        # Process the newest message that passed the filter
        filtered_messages.sort(
            key=lambda msg: int(msg.get("internalDate", 0)), reverse=True
        )
        return self._process_single_message(user_id, filtered_messages[0])

    def _process_single_message(self, user_id, message):
        """Process a single message from a thread"""
        message_id = message["id"]
        logger.debug(f"Processing message: {message_id}")

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

        # Initial count estimate - at minimum, this is the number of direct attachments
        attachment_count = len(attachments)

        # Create a simple preview of attachments for logging/diagnostics
        attachment_preview = [att.get("filename", "unknown") for att in attachments[:5]]
        if len(attachments) > 5:
            attachment_preview.append(f"... and {len(attachments) - 5} more")

        logger.info(
            f"Message has {attachment_count} direct attachments: {attachment_preview}"
        )

        # Check for potential archives that might significantly increase the file count
        archive_count = sum(
            1
            for att in attachments
            if any(
                att.get("filename", "").lower().endswith(ext)
                for ext in [".zip", ".rar", ".7z", ".tar", ".gz"]
            )
        )

        # Default fields for BigQuery compatibility
        default_attachment_fields = {
            "filename": "",
            "file_name": "",
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
            "processed": False,
            "skipped": False,
            "error": "",
            "file_type": "",
        }

        # If there are archives, we need to estimate how many files they might contain
        MAX_FILES_TO_PROCESS = 50

        # Simple heuristic: if there are more than 5 archives or more than 20 total attachments,
        # it's likely the total will exceed our limit
        likely_to_exceed = (archive_count > 5) or (attachment_count > 20)

        # If there are no archives, just use the direct count
        if archive_count == 0:
            likely_to_exceed = attachment_count > MAX_FILES_TO_PROCESS

        # Initialize file_limit_exceeded flag
        file_limit_exceeded = False

        if likely_to_exceed:
            logger.warning(
                f"Skipping attachment processing: message contains {attachment_count} attachments "
                f"including {archive_count} archives, which likely exceeds the limit of {MAX_FILES_TO_PROCESS} files"
            )

            # Create minimal attachment info with all required fields
            limited_attachments = []
            for att in attachments:
                att_info = default_attachment_fields.copy()
                att_info.update(
                    {
                        "filename": att.get("filename", "unknown"),
                        "file_name": att.get(
                            "filename", "unknown"
                        ),  # Set file_name to filename
                        "size": len(att.get("data", b"")),
                        "skipped": True,
                        "error": f"Estimated file count likely exceeds limit of {MAX_FILES_TO_PROCESS}",
                        "processed": False,
                        "file_type": self._get_file_extension(
                            att.get("filename", "unknown")
                        ),
                    }
                )
                limited_attachments.append(att_info)

            # Convert internalDate to readable format
            internal_date = None
            if "internalDate" in message:
                internal_date = datetime.fromtimestamp(
                    int(message["internalDate"]) / 1000
                ).strftime("%Y-%m-%d %H:%M:%S")

            return {
                "message_id": message_id,
                "thread_id": message.get("threadId", ""),
                "date": date,
                "internal_date": internal_date,
                "subject": subject,
                "from": from_email,
                "body": body,
                "summary": f"From: {from_email}\nSubject: {subject}\nDate: {date}\n\nBody Summary: {body[:200]}...",
                "attachments": limited_attachments,
                "direct_attachment_count": attachment_count,
                "archive_count": archive_count,
                "file_limit_exceeded": True,
            }

        # Process attachments normally if we don't expect to exceed the limit
        processed_attachments = []
        actual_file_count = 0

        # Process each attachment
        for i, att in enumerate(attachments):
            local_path = self.attachment_processor.save_attachment_to_file(att)
            if not local_path:
                # Add default fields for unsaved attachments
                att_info = default_attachment_fields.copy()
                att_info.update(
                    {
                        "filename": att.get("filename", "unknown"),
                        "file_name": att.get("filename", "unknown"),
                        "skipped": True,
                        "error": "Failed to save attachment",
                        "processed": False,
                        "file_type": self._get_file_extension(
                            att.get("filename", "unknown")
                        ),
                    }
                )
                processed_attachments.append(att_info)
                continue

            # Check if it's an archive
            if local_path.lower().endswith((".zip", ".rar", ".7z", ".tar", ".gz")):
                # Extract the archive
                extracted_files = self.attachment_processor.extract_archive(local_path)

                # Check if extraction increases our count beyond the limit
                if actual_file_count + len(extracted_files) > MAX_FILES_TO_PROCESS:
                    logger.warning(
                        f"Stopping attachment processing: archive extraction would increase file count to "
                        f"{actual_file_count + len(extracted_files)}, exceeding limit of {MAX_FILES_TO_PROCESS}"
                    )

                    # Add a note about the skipped archive - but still include it with all required fields
                    att_info = default_attachment_fields.copy()
                    att_info.update(
                        {
                            "filename": att.get("filename", "unknown"),
                            "file_name": att.get("filename", "unknown"),
                            "size": len(att.get("data", b"")),
                            "skipped": True,
                            "error": f"Archive extraction would exceed file limit of {MAX_FILES_TO_PROCESS}",
                            "processed": False,
                            "file_type": self._get_file_extension(
                                att.get("filename", "unknown")
                            ),
                            "extracted_count": len(extracted_files),
                        }
                    )
                    processed_attachments.append(att_info)

                    # Mark as exceeded but continue with what we have so far
                    file_limit_exceeded = True

                    # Process remaining attachments with minimal info but all required fields
                    for j in range(i + 1, len(attachments)):
                        remaining_att = attachments[j]
                        att_info = default_attachment_fields.copy()
                        att_info.update(
                            {
                                "filename": remaining_att.get("filename", "unknown"),
                                "file_name": remaining_att.get("filename", "unknown"),
                                "size": len(remaining_att.get("data", b"")),
                                "skipped": True,
                                "error": "Skipped due to file limit being reached",
                                "processed": False,
                                "file_type": self._get_file_extension(
                                    remaining_att.get("filename", "unknown")
                                ),
                            }
                        )
                        processed_attachments.append(att_info)

                    # Break out of the loop
                    break

                # Process the extracted files if under limit
                for file_path in extracted_files:
                    # Start with default fields to ensure all required fields exist
                    att_copy = default_attachment_fields.copy()
                    # Add original attachment info
                    att_copy.update(att)
                    # Make sure filename and file_name are set
                    if "filename" not in att_copy:
                        att_copy["filename"] = os.path.basename(file_path)
                    if "file_name" not in att_copy:
                        att_copy["file_name"] = os.path.basename(file_path)

                    processed_att = self._process_attachment_by_type(
                        att_copy, file_path
                    )

                    # Ensure file_name is set in processed result
                    if "file_name" not in processed_att and "filename" in processed_att:
                        processed_att["file_name"] = processed_att["filename"]

                    processed_attachments.append(processed_att)
                    actual_file_count += 1

                    # Check if we've hit the limit during extraction processing
                    if actual_file_count >= MAX_FILES_TO_PROCESS:
                        logger.warning(
                            f"Reached file processing limit of {MAX_FILES_TO_PROCESS} during extraction"
                        )
                        file_limit_exceeded = True
                        break

                # If we hit the limit during extraction, break the outer loop too
                if file_limit_exceeded:
                    # Process remaining attachments with minimal info
                    for j in range(i + 1, len(attachments)):
                        remaining_att = attachments[j]
                        att_info = default_attachment_fields.copy()
                        att_info.update(
                            {
                                "filename": remaining_att.get("filename", "unknown"),
                                "file_name": remaining_att.get("filename", "unknown"),
                                "size": len(remaining_att.get("data", b"")),
                                "skipped": True,
                                "error": "Skipped due to file limit being reached",
                                "processed": False,
                                "file_type": self._get_file_extension(
                                    remaining_att.get("filename", "unknown")
                                ),
                            }
                        )
                        processed_attachments.append(att_info)
                    break

            else:
                # Direct file handling - ensure all required fields exist
                att_copy = default_attachment_fields.copy()
                att_copy.update(att)
                # Make sure filename and file_name are consistent
                if "filename" not in att_copy:
                    att_copy["filename"] = os.path.basename(local_path)
                if "file_name" not in att_copy:
                    att_copy["file_name"] = att_copy["filename"]

                processed_att = self._process_attachment_by_type(att_copy, local_path)

                # Ensure file_name is set in processed result
                if "file_name" not in processed_att and "filename" in processed_att:
                    processed_att["file_name"] = processed_att["filename"]

                processed_attachments.append(processed_att)
                actual_file_count += 1

            # Check if we've reached the limit during processing
            if actual_file_count >= MAX_FILES_TO_PROCESS:
                logger.warning(
                    f"Reached file processing limit of {MAX_FILES_TO_PROCESS}"
                )
                file_limit_exceeded = True

                # Process remaining attachments with minimal info
                for j in range(i + 1, len(attachments)):
                    remaining_att = attachments[j]
                    att_info = default_attachment_fields.copy()
                    att_info.update(
                        {
                            "filename": remaining_att.get("filename", "unknown"),
                            "file_name": remaining_att.get("filename", "unknown"),
                            "size": len(remaining_att.get("data", b"")),
                            "skipped": True,
                            "error": "Skipped due to file limit being reached",
                            "processed": False,
                            "file_type": self._get_file_extension(
                                remaining_att.get("filename", "unknown")
                            ),
                        }
                    )
                    processed_attachments.append(att_info)

                break

        # Check all attachments for missing required fields
        for att in processed_attachments:
            # Ensure all default fields are present
            for field, default_value in default_attachment_fields.items():
                if field not in att:
                    att[field] = default_value

            # Make sure file_name is set if filename exists
            if "file_name" not in att and "filename" in att:
                att["file_name"] = att["filename"]

        # Replace attachments list
        attachments = processed_attachments

        if attachments:
            logger.info(f"Processed {len(attachments)} files from the message")

        # Create a summary of the email
        summary = f"From: {from_email}\nSubject: {subject}\nDate: {date}\n\nBody Summary: {body[:200]}..."

        # Convert internalDate to readable format
        internal_date = None
        if "internalDate" in message:
            internal_date = datetime.fromtimestamp(
                int(message["internalDate"]) / 1000
            ).strftime("%Y-%m-%d %H:%M:%S")
        return {
            "message_id": message_id,
            "thread_id": message.get("threadId", ""),
            "date": date,
            "internal_date": internal_date,
            "subject": subject,
            "from": from_email,
            "body": body,
            "summary": summary,
            "attachments": attachments,
            "processed_file_count": actual_file_count,
            "file_limit_exceeded": file_limit_exceeded,
        }

    def _get_file_extension(self, filename):
        """Extract file extension from filename"""
        if not filename:
            return ""

        file_extension = os.path.splitext(filename)[1].lower()
        # Remove the dot if present
        return file_extension[1:] if file_extension.startswith(".") else file_extension

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
            "processed": False,
            "skipped": False,
            "error": "",
        }
        # Update with values from process_result, using defaults for missing keys
        for key, default_value in default_values.items():
            att[key] = process_result.get(key, default_value)

        # Set file type based on extension
        file_extension = os.path.splitext(file_path)[1].lower()
        att["file_type"] = file_extension[1:]  # Remove the dot
        att["processed"] = True

        # Rename file if invoice number is available
        if process_result.get("document_number"):
            new_filename = f"{process_result.get('date', 'unknown')}_{process_result['document_number']}{file_extension}"
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

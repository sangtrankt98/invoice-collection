"""
Gmail handler module for fetching and processing emails
"""

import base64
import re
import logging
from googleapiclient.errors import HttpError
from utils.auth import GoogleAuthenticator
from utils.attachment_processor import AttachmentProcessor

# Set up logger
logger = logging.getLogger("invoice_collection.gmail")


class GmailHandler:
    """Handles Gmail API operations"""

    def __init__(self, credentials):
        """Initialize with Google credentials"""
        logger.info("Initializing Gmail handler")
        self.service = GoogleAuthenticator.create_service("gmail", "v1", credentials)
        self.attachment_processor = AttachmentProcessor(self.service)

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
        for att in attachments:
            local_path = self.attachment_processor.save_attachment_to_file(att)
            if local_path:
                # Process PDF with OCR or convert here
                process_result = self.attachment_processor.process_pdf(att)
                logger.info("Processed:", process_result)
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

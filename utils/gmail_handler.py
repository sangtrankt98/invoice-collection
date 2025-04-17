"""
Gmail handler module for fetching and processing emails
"""

import base64
import re
from googleapiclient.errors import HttpError
from utils.auth import GoogleAuthenticator
from utils.attachment_processor import AttachmentProcessor


class GmailHandler:
    """Handles Gmail API operations"""

    def __init__(self, credentials):
        """Initialize with Google credentials"""
        self.service = GoogleAuthenticator.create_service("gmail", "v1", credentials)
        self.attachment_processor = AttachmentProcessor(self.service)

    def extract_email_content(
        self, user_id="me", query="has:attachment", max_results=10
    ):
        """Fetch emails and extract content"""
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
                print("No messages found.")
                return []

            email_data = []
            for message in messages:
                msg = (
                    self.service.users()
                    .messages()
                    .get(userId=user_id, id=message["id"])
                    .execute()
                )

                # Process email
                email_info = self._process_email(user_id, message["id"], msg)
                email_data.append(email_info)

            return email_data

        except HttpError as error:
            print(f"An error occurred: {error}")
            return []

    def _process_email(self, user_id, message_id, message):
        """Process a single email message"""
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

        # Get email body
        body = self._get_email_body(message)

        # Find any Drive links in the email
        drive_links = self._extract_drive_links(body)

        # Extract attachments
        attachments = self.attachment_processor.get_attachments(
            user_id, message_id, message
        )

        # Create a summary of the email (simple version)
        summary = f"From: {from_email}\nSubject: {subject}\nDate: {date}\n\nBody Summary: {body[:200]}..."

        return {
            "message_id": message_id,
            "date": date,
            "subject": subject,
            "from": from_email,
            "body": body,
            "summary": summary,
            "drive_links": drive_links,
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

    @staticmethod
    def _extract_drive_links(text):
        """Extract Google Drive links from text"""
        drive_pattern = r"https://drive\.google\.com/\S+"
        return re.findall(drive_pattern, text)

"""
Module for processing email attachments
"""

import base64


class AttachmentProcessor:
    """Handles the processing of email attachments"""

    def __init__(self, gmail_service):
        """Initialize with Gmail service"""
        self.gmail_service = gmail_service

    def get_attachments(self, user_id, msg_id, message):
        """Get and process attachments from the message"""
        attachments = []

        def process_parts(parts):
            for part in parts:
                if "parts" in part:
                    process_parts(part["parts"])

                if "filename" in part and part["filename"]:
                    if "body" in part and "attachmentId" in part["body"]:
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

                        # Store attachment info
                        attachments.append(
                            {
                                "filename": file_name,
                                "data": file_data,
                                "size": len(file_data),
                                "mime_type": part["mimeType"],
                            }
                        )

        if "parts" in message["payload"]:
            process_parts(message["payload"]["parts"])

        return attachments

    def process_pdf(self, attachment):
        """Process PDF attachment - placeholder for future implementation"""
        # TODO: Implement PDF processing logic
        # This could use PyPDF2, pdfplumber, or other PDF libraries
        return {
            "type": "pdf",
            "filename": attachment["filename"],
            "size": attachment["size"],
            "content_summary": "PDF content summary placeholder",
        }

"""
Module for handling Google Drive operations
"""

# utils/drive_handler.py
import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from utils.auth import GoogleAuthenticator
import re
from utils.logger_setup import setup_logger

logger = setup_logger()


class DriveHandler:
    """Handles Google Drive API operations"""

    def __init__(self, credentials):
        """Initialize with Google credentials"""
        logger.info("Initializing Drive handler")
        self.service = GoogleAuthenticator.create_service("drive", "v3", credentials)

    def get_file_metadata(self, file_id):
        """Get metadata for a Drive file"""
        logger.info(f"Getting metadata for Drive file ID: {file_id}")
        try:
            file = self.service.files().get(fileId=file_id).execute()
            logger.info(f"Successfully retrieved metadata for file: {file.get('name')}")
            return file
        except Exception as e:
            logger.error(f"Error getting Drive file metadata for ID {file_id}: {e}")
            return None

    def download_file(self, file_id):
        """Download a file from Google Drive"""
        logger.info(f"Downloading file with ID: {file_id}")
        try:
            request = self.service.files().get_media(fileId=file_id)
            file_content = request.execute()
            logger.info(f"Successfully downloaded file ID: {file_id}")
            return file_content
        except Exception as e:
            logger.error(f"Error downloading file ID {file_id}: {e}")
            return None

    def extract_drive_folder_ids(self, text):
        """
        Extract folder ID from various forms of Google Drive links.

        Args:
            drive_link: Google Drive folder link

        Returns:
            str: Folder ID or None if not found
        """
        # Pattern for folder ID in Google Drive URLs
        patterns = [
            r"https://drive\.google\.com/drive/folders/([a-zA-Z0-9_-]+)(?:/.*)?",  # Standard folder link
            r"https://drive\.google\.com/drive/u/\d+/folders/([a-zA-Z0-9_-]+)(?:/.*)?",  # User-specific folder link
            r"https://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)",  # Open link
            r"https://docs\.google\.com/folder/d/([a-zA-Z0-9_-]+)(?:/.*)?",  # Old style folder link
            r"^([a-zA-Z0-9_-]{25,40})$",  # Direct folder ID
        ]

        folder_ids = set()
        for pattern in patterns:
            matches = re.findall(pattern, text)
            folder_ids.update(matches)

        return list(folder_ids)

    def list_files_in_folder(self, folder_id):
        """List all files in a given Drive folder"""
        try:
            response = (
                self.service.files()
                .list(
                    q=f"'{folder_id}' in parents and trashed = false",
                    fields="files(id, name, mimeType, size)",
                )
                .execute()
            )
            return response.get("files", [])
        except Exception as e:
            logger.error(f"Error listing files in folder {folder_id}: {e}")
            return []

    def verify_folder_access(self, folder_id):
        """
        Verify if the folder exists and is accessible.

        Args:
            folder_id: Google Drive folder ID

        Returns:
            bool: True if accessible, False otherwise
        """
        try:
            response = (
                self.service.files()
                .get(fileId=folder_id, fields="id, name, mimeType")
                .execute()
            )

            if response.get("mimeType") == "application/vnd.google-apps.folder":
                logger.info(
                    f"Successfully verified access to folder: {response.get('name')}"
                )
                return True
            else:
                logger.error(f"ID {folder_id} exists but is not a folder")
                return False

        except Exception as e:
            logger.error(f"Error accessing folder ID {folder_id}: {str(e)}")
            return False

    def get_or_create_folder(self, folder_name, parent_id=None):
        """
        Check if a folder exists in Google Drive, if not create it.

        Args:
            folder_name: Name of the folder
            parent_id: ID of the parent folder (optional)

        Returns:
            str: Folder ID
        """
        query = (
            f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
        )
        if parent_id:
            query += f" and '{parent_id}' in parents"

        response = (
            self.service.files()
            .list(q=query, spaces="drive", fields="files(id, name)")
            .execute()
        )

        folders = response.get("files", [])

        # Return existing folder if found
        if folders:
            logger.info(f"Found existing folder: {folder_name}")
            return folders[0]["id"]

        # Create new folder
        folder_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }

        if parent_id:
            folder_metadata["parents"] = [parent_id]

        folder = (
            self.service.files().create(body=folder_metadata, fields="id").execute()
        )

        folder_id = folder.get("id")
        # logger.info(f"Created new folder: {folder_name} (ID: {folder_id})")
        return folder_id

    def upload_file(self, file_path, folder_id, file_name=None):
        """
        Upload a file to a specified folder in Google Drive.

        Args:
            file_path: Path to the file to upload
            folder_id: ID of the folder to upload to
            file_name: Name to give the file in Drive (optional)

        Returns:
            str: File ID if successful, None otherwise
        """
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None

        if file_name is None:
            file_name = os.path.basename(file_path)

        file_metadata = {"name": file_name, "parents": [folder_id]}

        media = MediaFileUpload(file_path, resumable=True)

        try:
            file = (
                self.service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )

            file_id = file.get("id")
            logger.info(f"Uploaded file: {file_name} to folder ID: {folder_id}")
            return file_id
        except Exception as e:
            logger.error(f"Error uploading file {file_name}: {str(e)}")
            return None

    def organize_and_upload_pdfs(self, drive_link, email_data, local_pdf_dir):
        """
        Organize PDF files by company and upload them to Google Drive.

        Args:
            drive_link: Google Drive folder link or ID
            email_data: List of dictionaries containing email data with company info
            local_pdf_dir: Directory where PDF files are saved locally

        Returns:
            dict: Summary of upload results
        """
        results = {"successful_uploads": 0, "failed_uploads": 0, "company_folders": {}}

        # Extract and verify folder ID
        master_folder_id = self.extract_drive_folder_ids(drive_link)[0]
        if not master_folder_id:
            logger.error("Invalid Google Drive link provided")
            results["error"] = "Invalid Google Drive link provided"
            return results

        if not self.verify_folder_access(master_folder_id):
            logger.error("Cannot access the provided Google Drive folder")
            results["error"] = "Cannot access the provided Google Drive folder"
            return results
        # Initialize results
        results = {"successful_uploads": 0, "failed_uploads": 0, "company_folders": {}}
        # Group rows by entity_name
        grouped = email_data.groupby("entity_name")
        # Process each company
        for entity_name, group_df in grouped:
            # Get or create folder
            company_folder_id = self.get_or_create_folder(entity_name, master_folder_id)
            results["company_folders"][entity_name] = company_folder_id
            for _, row in group_df.iterrows():
                filename = row.get("file_naming")
                if not filename:
                    continue

                full_path = os.path.join(local_pdf_dir, filename)
                if not os.path.exists(full_path):
                    # or not full_path.lower().endswith(".pdf")
                    logger.warning(f"File missing or invalid: {full_path}")
                    continue

                file_id = self.upload_file(full_path, company_folder_id)

                if file_id:
                    results["successful_uploads"] += 1
                else:
                    results["failed_uploads"] += 1

        logger.info(
            f"Upload summary: {results['successful_uploads']} successful, {results['failed_uploads']} failed"
        )
        return results

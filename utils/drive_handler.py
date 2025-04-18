"""
Module for handling Google Drive operations
"""

import logging
from utils.auth import GoogleAuthenticator

# Set up logger
logger = logging.getLogger("invoice_collection.drive")


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

    def extract_file_id_from_url(self, url):
        """Extract file ID from Drive URL"""
        logger.debug(f"Extracting file ID from URL: {url}")
        # Example URL: https://drive.google.com/file/d/FILE_ID/view
        # or https://drive.google.com/open?id=FILE_ID
        if "/file/d/" in url:
            file_id = url.split("/file/d/")[1].split("/")[0]
            logger.debug(f"Extracted file ID: {file_id} from direct link")
        elif "id=" in url:
            file_id = url.split("id=")[1].split("&")[0]
            logger.debug(f"Extracted file ID: {file_id} from query parameter")
        else:
            logger.warning(f"Could not extract file ID from URL: {url}")
            file_id = None
        return file_id

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

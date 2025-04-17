"""
Module for handling Google Drive operations
"""

from utils.auth import GoogleAuthenticator


class DriveHandler:
    """Handles Google Drive API operations"""

    def __init__(self, credentials):
        """Initialize with Google credentials"""
        self.service = GoogleAuthenticator.create_service("drive", "v3", credentials)

    def get_file_metadata(self, file_id):
        """Get metadata for a Drive file"""
        try:
            file = self.service.files().get(fileId=file_id).execute()
            return file
        except Exception as e:
            print(f"Error getting Drive file metadata: {e}")
            return None

    def extract_file_id_from_url(self, url):
        """Extract file ID from Drive URL"""
        # Example URL: https://drive.google.com/file/d/FILE_ID/view
        # or https://drive.google.com/open?id=FILE_ID
        if "/file/d/" in url:
            file_id = url.split("/file/d/")[1].split("/")[0]
        elif "id=" in url:
            file_id = url.split("id=")[1].split("&")[0]
        else:
            file_id = None
        return file_id

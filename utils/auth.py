"""
Authentication module for Google APIs
"""

import os
import pickle
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import config


class GoogleAuthenticator:
    """Handles authentication with Google APIs"""

    def __init__(self, scopes):
        """Initialize the authenticator with required scopes"""
        self.scopes = scopes
        self.credentials_file = config.CREDENTIALS_FILE
        self.token_file = config.TOKEN_FILE

    def get_credentials(self):
        """Get authenticated credentials"""
        creds = None

        # Check if token file exists
        if os.path.exists(self.token_file):
            with open(self.token_file, "rb") as token:
                creds = pickle.load(token)

        # If credentials don't exist or are invalid, authenticate
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, self.scopes
                )
                creds = flow.run_local_server(port=0)

            # Save the credentials for next run
            with open(self.token_file, "wb") as token:
                pickle.dump(creds, token)

        return creds

    @staticmethod
    def create_service(service_name, version, credentials):
        """Create a Google API service"""
        return build(service_name, version, credentials=credentials)

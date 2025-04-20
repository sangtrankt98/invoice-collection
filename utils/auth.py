"""
Authentication module for Google APIs using current Google API Python Client libraries
"""

import os
import json
import logging
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import config
import tempfile

# Set up logger
logger = logging.getLogger("invoice_collection.auth")
client_secrets_dict = {
    "installed": {
        "client_id": "208149596709-mbhvmgfr1jsj1p8hkssn3hs99t3qf1mj.apps.googleusercontent.com",
        "project_id": "immortal-0804",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": "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",
        "redirect_uris": ["http://localhost"],
    }
}


class GoogleAuthenticator:
    """Handles authentication with Google APIs"""

    def __init__(self, scopes):
        """Initialize the authenticator with required scopes"""
        self.scopes = scopes
        self.credentials_file = config.CREDENTIALS_FILE
        self.token_file = config.TOKEN_FILE

    def get_credentials(self):
        """Get authenticated credentials using latest Google Auth libraries"""
        logger.info("Getting credentials")
        creds = None

        # Check if we have a token file
        if os.path.exists(self.token_file):
            logger.info("Loading credentials from token file")
            try:
                with open(self.token_file, "r") as token:
                    creds = Credentials.from_authorized_user_info(
                        json.load(token), self.scopes
                    )
            except Exception as e:
                logger.error(f"Error loading credentials from token file: {e}")

        # Check if we need to refresh credentials or create new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.info("Refreshing expired credentials")
                try:
                    creds.refresh(Request())
                except Exception as e:
                    logger.error(f"Error refreshing credentials: {e}")
                    creds = None

            # If still no valid credentials, start OAuth flow
            if not creds or not creds.valid:
                logger.info("Starting OAuth flow for new credentials")
                try:
                    # Write to a temp file
                    with tempfile.NamedTemporaryFile(
                        mode="w+", delete=False, suffix=".json"
                    ) as temp:
                        json.dump(client_secrets_dict, temp)
                        temp.flush()  # Ensure it's written to disk
                        creds = InstalledAppFlow.from_client_secrets_file(
                            temp.name, scopes=self.scopes
                        ).run_local_server(port=0)
                    logger.info("Successfully obtained new credentials")
                except Exception as e:
                    logger.error(f"Error in OAuth flow: {e}")
                    raise

            # Save credentials to token file
            try:
                with open(self.token_file, "w") as token:
                    token.write(creds.to_json())
                logger.info("Credentials saved to token file")
            except Exception as e:
                logger.error(f"Error saving credentials: {e}")

        return creds

    def get_service_account_credentials(self):
        """Get credentials from service account key file"""
        logger.info("Getting service account credentials")
        try:
            creds = service_account.Credentials.from_service_account_file(
                self.credentials_file, scopes=self.scopes
            )
            logger.info("Successfully loaded service account credentials")
            return creds
        except Exception as e:
            logger.error(f"Error loading service account credentials: {e}")
            raise

    @staticmethod
    def create_service(service_name, version, credentials):
        """Create a Google API service"""
        logger.info(f"Creating {service_name} service (version {version})")
        try:
            service = build(service_name, version, credentials=credentials)
            logger.info(f"{service_name} service created successfully")
            return service
        except Exception as e:
            logger.error(f"Error creating {service_name} service: {e}")
            raise

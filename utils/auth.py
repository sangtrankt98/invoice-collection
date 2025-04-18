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

# Set up logger
logger = logging.getLogger("invoice_collection.auth")


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
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_file, self.scopes
                    )
                    creds = flow.run_local_server(port=0)
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

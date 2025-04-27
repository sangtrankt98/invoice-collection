"""
Module for handling local directory operations
Parallels the functionality of DriveHandler but for local filesystem
"""

import os
import shutil
import logging
import re
from pathlib import Path
from utils.logger_setup import setup_logger

logger = setup_logger()


class LocalHandler:
    """Handles local directory operations similar to Google Drive operations"""

    def __init__(self, base_directory=None):
        """Initialize with base directory"""
        logger.info("Initializing Local Directory handler")
        self.base_directory = base_directory or os.getcwd()
        # Ensure base directory exists
        os.makedirs(self.base_directory, exist_ok=True)

    def get_file_metadata(self, file_path):
        """Get metadata for a local file"""
        file_path = (
            os.path.join(self.base_directory, file_path)
            if not os.path.isabs(file_path)
            else file_path
        )
        logger.info(f"Getting metadata for local file: {file_path}")
        try:
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return None

            stat_info = os.stat(file_path)
            file_info = {
                "name": os.path.basename(file_path),
                "path": file_path,
                "size": stat_info.st_size,
                "created": stat_info.st_ctime,
                "modified": stat_info.st_mtime,
                "accessed": stat_info.st_atime,
            }
            logger.info(
                f"Successfully retrieved metadata for file: {file_info['name']}"
            )
            return file_info
        except Exception as e:
            logger.error(f"Error getting file metadata for {file_path}: {e}")
            return None

    def read_file(self, file_path):
        """Read a file from local directory"""
        file_path = (
            os.path.join(self.base_directory, file_path)
            if not os.path.isabs(file_path)
            else file_path
        )
        logger.info(f"Reading file: {file_path}")
        try:
            with open(file_path, "rb") as f:
                file_content = f.read()
            logger.info(f"Successfully read file: {file_path}")
            return file_content
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return None

    def extract_directory_path(self, path_text):
        """
        Extract directory path from various forms of input.

        Args:
            path_text: Path string which could be relative or absolute

        Returns:
            str: Normalized directory path or None if not valid
        """
        if not path_text:
            return None

        # Clean and normalize the path
        try:
            # Handle both Windows and Unix paths
            path = os.path.normpath(path_text)

            # Check if this is a directory
            if os.path.isdir(path):
                return path

            # Check if it's a parent directory that exists
            parent_dir = os.path.dirname(path)
            if os.path.isdir(parent_dir):
                return parent_dir

            # See if it's a relative path from base directory
            relative_path = os.path.join(self.base_directory, path_text)
            if os.path.isdir(relative_path):
                return relative_path

            logger.warning(f"Could not resolve directory path: {path_text}")
            return None
        except Exception as e:
            logger.error(f"Error parsing directory path {path_text}: {e}")
            return None

    def list_files_in_directory(self, directory_path):
        """List all files in a given directory"""
        directory_path = (
            os.path.join(self.base_directory, directory_path)
            if not os.path.isabs(directory_path)
            else directory_path
        )
        try:
            if not os.path.exists(directory_path):
                logger.error(f"Directory does not exist: {directory_path}")
                return []

            files = []
            for entry in os.scandir(directory_path):
                if entry.is_file():
                    stats = entry.stat()
                    files.append(
                        {
                            "id": entry.path,  # Use full path as ID for local files
                            "name": entry.name,
                            "mimeType": "directory" if entry.is_dir() else "file",
                            "size": stats.st_size,
                        }
                    )
            return files
        except Exception as e:
            logger.error(f"Error listing files in directory {directory_path}: {e}")
            return []

    def verify_directory_access(self, directory_path):
        """
        Verify if the directory exists and is accessible.

        Args:
            directory_path: Local directory path

        Returns:
            bool: True if accessible, False otherwise
        """
        directory_path = (
            os.path.join(self.base_directory, directory_path)
            if not os.path.isabs(directory_path)
            else directory_path
        )
        try:
            if not os.path.exists(directory_path):
                logger.error(f"Directory does not exist: {directory_path}")
                return False

            if not os.path.isdir(directory_path):
                logger.error(f"Path exists but is not a directory: {directory_path}")
                return False

            # Check if we have read/write access
            test_access = os.access(directory_path, os.R_OK | os.W_OK)
            if not test_access:
                logger.error(
                    f"Insufficient permissions for directory: {directory_path}"
                )
                return False

            logger.info(f"Successfully verified access to directory: {directory_path}")
            return True
        except Exception as e:
            logger.error(f"Error accessing directory {directory_path}: {str(e)}")
            return False

    def get_or_create_directory(self, directory_name, parent_dir=None):
        """
        Check if a directory exists, if not create it.

        Args:
            directory_name: Name of the directory
            parent_dir: Path of the parent directory (optional)

        Returns:
            str: Directory path
        """
        # Determine parent directory
        if parent_dir:
            parent_dir = (
                os.path.join(self.base_directory, parent_dir)
                if not os.path.isabs(parent_dir)
                else parent_dir
            )
        else:
            parent_dir = self.base_directory

        # Full path to the target directory
        target_dir = os.path.join(parent_dir, directory_name)

        # Check if directory exists
        if os.path.exists(target_dir) and os.path.isdir(target_dir):
            logger.info(f"Found existing directory: {target_dir}")
            return target_dir

        # Create new directory
        try:
            os.makedirs(target_dir, exist_ok=True)
            logger.info(f"Created new directory: {target_dir}")
            return target_dir
        except Exception as e:
            logger.error(f"Error creating directory {target_dir}: {str(e)}")
            return None

    def copy_file(self, source_path, target_dir, file_name=None):
        """
        Copy a file to a specified directory.

        Args:
            source_path: Path to the file to copy
            target_dir: Directory to copy to
            file_name: Name to give the file in target (optional)

        Returns:
            str: New file path if successful, None otherwise
        """
        if not os.path.exists(source_path):
            logger.error(f"Source file not found: {source_path}")
            return None

        # Ensure target directory exists
        if not os.path.exists(target_dir):
            logger.error(f"Target directory not found: {target_dir}")
            return None

        # Determine target file name
        if file_name is None:
            file_name = os.path.basename(source_path)

        target_path = os.path.join(target_dir, file_name)

        try:
            shutil.copy2(source_path, target_path)
            logger.info(f"Copied file: {source_path} to {target_path}")
            return target_path
        except Exception as e:
            logger.error(f"Error copying file {source_path}: {str(e)}")
            return None

    def organize_and_copy_pdfs(self, target_base_dir, email_data, source_pdf_dir):
        """
        Organize PDF files by company and copy them to directories.

        Args:
            target_base_dir: Base directory where company folders will be created
            email_data: DataFrame containing email data with company info
            source_pdf_dir: Directory where PDF files are currently saved

        Returns:
            dict: Summary of copy results
        """
        results = {"successful_copies": 0, "failed_copies": 0, "company_folders": {}}

        # Verify or create the base directory
        if not target_base_dir:
            target_base_dir = os.path.join(self.base_directory, "organized_pdfs")

        if not os.path.isabs(target_base_dir):
            target_base_dir = os.path.join(self.base_directory, target_base_dir)

        # Create base directory if it doesn't exist
        os.makedirs(target_base_dir, exist_ok=True)

        if not self.verify_directory_access(target_base_dir):
            logger.error("Cannot access the target base directory")
            results["error"] = "Cannot access the target base directory"
            return results

        # Group rows by entity_name
        grouped = email_data.groupby("entity_name")

        # Process each company
        for entity_name, group_df in grouped:
            # Create company directory
            company_dir = self.get_or_create_directory(entity_name, target_base_dir)
            results["company_folders"][entity_name] = company_dir

            # Process each file for this company
            for _, row in group_df.iterrows():
                filename = row.get("file_naming")
                if not filename:
                    continue

                source_path = os.path.join(source_pdf_dir, filename)
                if not os.path.exists(source_path):
                    logger.warning(f"Source file missing: {source_path}")
                    continue

                # Copy the file
                new_file_path = self.copy_file(source_path, company_dir)

                if new_file_path:
                    results["successful_copies"] += 1
                else:
                    results["failed_copies"] += 1

        logger.info(
            f"Copy summary: {results['successful_copies']} successful, {results['failed_copies']} failed"
        )
        return results

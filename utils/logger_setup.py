"""
Logger configuration module
"""

import logging
import os
from datetime import datetime


def setup_logger(log_file=None):
    """Configure and return the application logger"""
    # Create logs directory if it doesn't exist
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Create default log filename if none provided
    if log_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"logs/invoice_collection_{timestamp}.log"

    # Create logger
    logger = logging.getLogger("invoice_collection")
    logger.setLevel(logging.INFO)

    # Clear any existing handlers
    if logger.handlers:
        logger.handlers.clear()

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create file handler
    file_handler = logging.FileHandler(log_file, "w", "utf-8")
    file_handler.setLevel(logging.DEBUG)  # More verbose logging to file

    # Create formatters
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Add formatters to handlers
    console_handler.setFormatter(console_formatter)
    file_handler.setFormatter(file_formatter)

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

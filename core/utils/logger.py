"""
Zypher Logger - Centralized Logging Utility
"""
import logging
import sys

def setup_logger():
    # Create a custom logger
    logger = logging.getLogger("zypher")
    logger.setLevel(logging.DEBUG)

    # Create handlers
    c_handler = logging.StreamHandler(sys.stdout)
    c_handler.setLevel(logging.INFO)

    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(message)s') # Clean output for CLI
    c_handler.setFormatter(c_format)

    # Add handlers to the logger
    if not logger.handlers:
        logger.addHandler(c_handler)

    return logger

# Initialize singleton
logger = setup_logger()

__all__ = ["logger"]
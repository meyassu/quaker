import logging
import sys
import os


"""
Package-wide Constants
"""
TOP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INTERNAL_DATA_DIR = os.path.join(TOP_DIR, 'data/internal/')
LOGS_DIR = os.path.join(TOP_DIR, 'logs')

def configure_py():
    """
    Adds top-level directory to sys.path.

    :return: (bool) -> indicates the success of the operation
    """
    
    try:
        if TOP_DIR not in sys.path:
            sys.path.append(TOP_DIR)
    except Exception as e:
        raise

    return True

def create_logger():
    """
    Create a logger.

    :return: (bool) -> indicates the success of the operation
    """
    
    # Create logs directory if it doesnt exist
    try:
        if not os.path.exists(LOGS_DIR):
            os.makedirs(LOGS_DIR)
    except Exception as e:
        raise

    # Specify path to log file
    logs_fpath = os.path.join(LOGS_DIR, 'log.txt')

    # Set logging configuration
    logger = None
    try:
        logger = logging.getLogger('rgc')
        logger.setLevel(logging.INFO)

        # Add handler to the logger
        fhandler = logging.FileHandler(logs_fpath)
        fhandler.setLevel(logging.INFO)

        # Create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fhandler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(fhandler)
    except Exception as e:
        raise

    return logger



"""
Initial Configuration
"""
LOGGER = create_logger()





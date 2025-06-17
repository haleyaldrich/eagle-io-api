import logging
import logging.handlers
import os
from datetime import datetime

def setup_logging(
    log_level=logging.INFO,
    log_directory="logs",
    app_name="myapp"
):
    """
    Sets up logging configuration for the application.
    
    Args:
        log_level: The logging level to use (default: logging.INFO)
        log_directory: Directory to store log files (default: 'logs')
        app_name: Name of the application for log file naming (default: 'myapp')
    """
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    timestamp = datetime.now().strftime('%Y%m%d')
    log_filename = f"{app_name}_{timestamp}.log"
    log_filepath = os.path.join(log_directory, log_filename)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    file_handler = logging.handlers.RotatingFileHandler(
        log_filepath,
        maxBytes=10485760,  # 10MB
        backupCount=5, # Keeps 5 backup files before deleting old ones
        mode='w',  # Overwrite the log file each time
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    root_logger.handlers = []
    
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Set specific log levels for third-party libraries
    DISABLED = 60 # Disables logging for the specified library
    logging.getLogger('httpcore').setLevel(DISABLED)
    logging.getLogger('httpx').setLevel(DISABLED)
    logging.getLogger('urllib3').setLevel(DISABLED)
    logging.getLogger('matplotlib').setLevel(DISABLED)
    logging.getLogger('PIL').setLevel(DISABLED)

    logger = logging.getLogger(__name__)
    return logger
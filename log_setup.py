"""
Logging setup.
Log to screen and file.
"""

import logging
import logging.handlers


def get_logger():
    # For console logging.
    logging.basicConfig(
        level=logging.WARNING,
        format='[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    )
    # File handler and formatting.
    handler = logging.handlers.RotatingFileHandler(
        "data/etl.log",
        maxBytes=10*1024*1024,
        backupCount=5,
    )
    lformat = logging.Formatter("%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")
    handler.setFormatter(lformat)

    # Module logging
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    backend = logging.getLogger("backend")
    backend.setLevel(logging.INFO)
    backend.addHandler(handler)
    # Harvest/load scripts logging
    logger = logging.getLogger('rap')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger
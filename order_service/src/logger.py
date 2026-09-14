import logging
import os


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", logging.INFO),
    format="%(levelname)s | %(asctime)s | %(message)s",
)

logger = logging.getLogger()
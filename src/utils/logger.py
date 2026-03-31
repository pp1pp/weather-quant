import sys
from loguru import logger

# Remove default handler
logger.remove()

# Console output
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    level="DEBUG",
)

# File output with daily rotation, 30-day retention
logger.add(
    "data/logs/system.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function} - {message}",
    level="DEBUG",
    rotation="1 day",
    retention="30 days",
    encoding="utf-8",
)

import os
from prozorro_crawler.settings import logger, PUBLIC_API_HOST


LOGGER = logger

API_HOST = os.environ.get("API_HOST", PUBLIC_API_HOST)
API_OPT_FIELDS = os.environ.get("API_OPT_FIELDS", "status,lots,procurementMethodType").split(",")
API_TOKEN = os.environ.get("API_TOKEN", "contracting")

ERROR_INTERVAL = int(os.environ.get("ERROR_INTERVAL", 5))

JOURNAL_PREFIX = os.environ.get("JOURNAL_PREFIX", "JOURNAL_")

SENTRY_DSN = os.getenv("SENTRY_DSN")

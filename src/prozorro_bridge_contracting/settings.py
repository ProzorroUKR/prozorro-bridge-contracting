import os
from prozorro_crawler.settings import logger, CRAWLER_USER_AGENT


MONGODB_URL = os.environ.get("MONGODB_URL", "mongodb://root:example@localhost:27017")
MONGODB_DATABASE = os.environ.get("MONGODB_DATABASE", "prozorro-bridge-contracting")
MONGODB_CONTRACTS_COLLECTION = os.environ.get("MONGODB_CONTRACTS_COLLECTION", "contracts")
ERROR_INTERVAL = int(os.environ.get("ERROR_INTERVAL", 5))

API_OPT_FIELDS = os.environ.get("API_OPT_FIELDS", "status,lots,procurementMethodType")
PUBLIC_API_HOST = os.environ.get("PUBLIC_API_HOST", "https://lb-api-sandbox-2.prozorro.gov.ua")
API_VERSION = os.environ.get("API_VERSION", "2.5")
BASE_URL = f"{PUBLIC_API_HOST}/api/{API_VERSION}"
API_TOKEN = os.environ.get("API_TOKEN", "contracting")
SENTRY_DSN = os.getenv("SENTRY_DSN")

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_TOKEN}",
    "User-Agent": CRAWLER_USER_AGENT,
}

LOGGER = logger

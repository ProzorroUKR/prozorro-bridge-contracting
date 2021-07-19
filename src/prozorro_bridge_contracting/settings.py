import os
from prozorro_crawler.settings import logger


MONGODB_URL = os.environ.get("MONGODB_URL", "mongodb://root:example@localhost:27017")
MONGODB_HOST = ":".join(MONGODB_URL.split(":")[:-1])
MONGODB_DATABASE = os.environ.get("MONGODB_DATABASE", "prozorro-chronograph")
API_OPT_FIELDS = "status,lots,procurementMethodType"

PUBLIC_API_HOST = os.environ.get("PUBLIC_API_HOST", "https://lb-api-sandbox-2.prozorro.gov.ua")
API_VERSION = os.environ.get("API_VERSION", "2.5")
BASE_URL = f"{PUBLIC_API_HOST}/api/{API_VERSION}/tenders"
API_TOKEN = os.environ.get("API_TOKEN", "chronograph")
SANDBOX_MODE = os.environ.get("SANDBOX_MODE", False)

LOGGER = logger

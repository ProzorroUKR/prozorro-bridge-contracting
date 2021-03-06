from aiohttp import ClientSession
import argparse
import asyncio
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from prozorro_crawler.main import main

from prozorro_bridge_contracting.bridge import process_listing
from prozorro_bridge_contracting.single import sync_single_tender
from prozorro_bridge_contracting.settings import SENTRY_DSN


API_OPT_FIELDS = (
    "status",
    "lots",
    "contracts",
    "procurementMethodType",
    "procuringEntity",
    "mode",
)


async def data_handler(session: ClientSession, items: list) -> None:
    process_items_tasks = []
    for item in items:
        coroutine = process_listing(session, item)
        process_items_tasks.append(coroutine)
    await asyncio.gather(*process_items_tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Contracting Data Bridge")
    parser.add_argument("--tender", type=str, help="Tender id to sync", dest="tender_id")
    params = parser.parse_args()
    if SENTRY_DSN:
        sentry_sdk.init(
            dsn=SENTRY_DSN,
            integrations=[AioHttpIntegration()]
        )
    if params.tender_id:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(sync_single_tender(tender_id=params.tender_id))
    else:
        main(data_handler, opt_fields=API_OPT_FIELDS)

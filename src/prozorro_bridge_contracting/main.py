from aiohttp import ClientSession
import argparse
import asyncio
from prozorro_crawler.main import main

from prozorro_bridge_contracting.bridge import process_listing, sync_single_tender


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
    if params.tender_id:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(sync_single_tender(tender_id=params.tender_id))
    else:
        main(data_handler)

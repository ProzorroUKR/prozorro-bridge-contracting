import asyncio
from aiohttp import ClientSession
from prozorro_crawler.main import main

from .settings import PUBLIC_API_HOST
from .bridge import process_listing


async def data_handler(session: ClientSession, items: list) -> None:
    server_id_cookie = getattr(
        session.cookie_jar.filter_cookies(PUBLIC_API_HOST).get("SERVER_ID"), "value", None
    )
    process_items_tasks = []
    for item in items:
        coroutine = process_listing(server_id_cookie, item)
        process_items_tasks.append(coroutine)
    await asyncio.gather(*process_items_tasks)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    main(data_handler)

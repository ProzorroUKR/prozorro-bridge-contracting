import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError
from typing import Callable

from prozorro_bridge_contracting.journal_msg_ids import MONGODB_EXCEPTION
from prozorro_bridge_contracting.settings import (
    MONGODB_CONTRACTS_COLLECTION,
    MONGODB_DATABASE,
    MONGODB_URL,
    LOGGER,
    ERROR_INTERVAL,
)


def retry_decorator(log_message: str = None) -> Callable:
    def outer(func: Callable) -> Callable:
        async def inner(*args, **kwargs) -> None:
            while True:
                try:
                    await func(*args, **kwargs)
                    break
                except PyMongoError as e:
                    LOGGER.warning({"message": log_message, "error": e}, extra={"MESSAGE_ID": MONGODB_EXCEPTION})
                    await asyncio.sleep(ERROR_INTERVAL)
        return inner
    return outer


class Db:
    def __init__(self):
        self.client = AsyncIOMotorClient(MONGODB_URL)
        self.db = getattr(self.client, MONGODB_DATABASE)
        self.collection = getattr(self.db, MONGODB_CONTRACTS_COLLECTION)

    @retry_decorator(log_message="Get item from contracts")
    async def get(self, key: str) -> dict:
        return await self.collection.find_one({"_id": key})

    @retry_decorator(log_message="Put item in contracts")
    async def put(self, key: str, value) -> None:
        await self.collection.update_one(
            {"_id": key},
            {"$set": {"_id": key, "value": value}},
            upsert=True
        )

    @retry_decorator(log_message="Exists in contracts")
    async def has(self, key: str) -> bool:
        value = await self.collection.find_one({"_id": key})
        return value is not None

    @retry_decorator(log_message="get_tender_contracts_fb")
    async def get_tender_contracts_fb(self, tender: dict) -> bool:
        stored = await self.get(tender["id"])
        if stored and stored.get("value", None) == tender["dateModified"]:
            return False
        return True

    @retry_decorator(log_message="put_tender_in_cache_by_contract")
    async def put_tender_in_cache_by_contract(self, tender_id: str, dateModified: str = None) -> None:
        if dateModified:
            await self.put(tender_id, dateModified)

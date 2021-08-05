from aiohttp import ClientSession
import asyncio
import json

from prozorro_crawler.storage import get_feed_position
from prozorro_bridge_contracting.db import Db
from prozorro_bridge_contracting.settings import BASE_URL, LOGGER, API_TOKEN, ERROR_INTERVAL
from prozorro_bridge_contracting.utils import journal_context, extend_contract, check_tender
from prozorro_bridge_contracting.journal_msg_ids import (
    DATABRIDGE_EXCEPTION,
    DATABRIDGE_GET_CREDENTIALS,
    DATABRIDGE_GOT_CREDENTIALS,
    DATABRIDGE_CONTRACT_TO_SYNC,
    DATABRIDGE_CONTRACT_EXISTS,
    DATABRIDGE_CREATE_CONTRACT,
    DATABRIDGE_CONTRACT_CREATED,
    DATABRIDGE_CACHED,
)

cache_db = Db()
SESSION = ClientSession()


async def sync_single_tender(tender_id: str) -> None:
    feed_position = await get_feed_position()
    server_id = feed_position.get("server_id") if feed_position else None
    SESSION.cookie_jar.update_cookies({"SERVER_ID": server_id})

    transferred_contracts = []
    try:
        LOGGER.info(f"Getting tender {tender_id}")
        tender = await get_tender(tender_id)
        LOGGER.info(f"Got tender {tender['id']} in status {tender['status']}")

        LOGGER.info(f"Getting tender {tender_id} credentials")
        tender_credentials = await get_tender_credentials(tender_id)
        LOGGER.info(f"Got tender {tender['id']} credentials")

        for contract in tender.get("contracts", []):
            if contract["status"] != "active":
                LOGGER.info(f"Skip contract {contract['id']} in status {contract['status']}")
                continue

            LOGGER.info(f"Checking if contract {contract['id']} already exists")
            response = await SESSION.get(f"{BASE_URL}/contracts/{contract['id']}")
            if response.status == 200:
                LOGGER.info(f"Contract exists {contract['id']}")
                continue
            LOGGER.info(f"Contract {contract['id']} does not exists. Prepare contract for creation.")

            LOGGER.info(f"Extending contract {contract['id']} with extra data")
            extend_contract(contract, tender)
            await prepare_contract_data(contract, tender_credentials)

            LOGGER.info(f"Creating contract {contract['id']}")
            response = await SESSION.post(
                f"{BASE_URL}/contracts/{contract['id']}",
                json={"data": contract},
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {API_TOKEN}",
                },
            )
            data = await response.text()
            if response.status == 422:
                raise ValueError(data)
            elif response.status == (403, 410, 404, 405):
                raise PermissionError(data)
            elif response.status != 201:
                raise ConnectionError(data)
            else:
                response = await response.json()
            assert "data" in response
            LOGGER.info(f"Contract {contract['id']} created")
            transferred_contracts.append(contract["id"])
    except Exception as e:
        LOGGER.exception(e)
        raise
    else:
        if transferred_contracts:
            LOGGER.info(f"Successfully transfered contracts: {transferred_contracts}")
        else:
            LOGGER.info(f"Tender {tender_id} does not contain contracts to transfer")


async def get_tender_credentials(tender_id: str) -> dict:
    url = f"{BASE_URL}/{tender_id}/extract_credentials"
    while True:
        LOGGER.info(
            f"Getting credentials for tender {tender_id}",
            extra=journal_context(
                {"MESSAGE_ID": DATABRIDGE_GET_CREDENTIALS},
                {"TENDER_ID": tender_id}
            ),
        )
        try:
            response = await SESSION.get(
                url,
                headers={
                    "Authorization": f"Bearer {API_TOKEN}",
                }
            )
            data = await response.text()
            if response.status == 200:
                data = json.loads(data)
                LOGGER.info(
                    f"Got tender {tender_id} credentials",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_GOT_CREDENTIALS},
                        {"TENDER_ID": tender_id}
                    ),
                )
                return data
            raise ConnectionError(f"Failed to get credentials {data}")
        except Exception as e:
            LOGGER.warning(
                f"Can't get tender credentials {tender_id}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                    {"TENDER_ID": tender_id}
                ),
            )
            LOGGER.exception(e)
            await asyncio.sleep(ERROR_INTERVAL)


async def get_tender(tender_id: str) -> dict:
    while True:
        try:
            response = await SESSION.get(
                f"{BASE_URL}/{tender_id}",
                headers={
                    "Authorization": f"Bearer {API_TOKEN}",
                }
            )
            data = await response.text()
            if response.status != 200:
                raise ConnectionError(f"Error {data}")
            return json.loads(data)["data"]
        except Exception as e:
            LOGGER.warning(
                f"Fail to get tender {tender_id}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                    params={"TENDER_ID": tender_id}
                )
            )
            LOGGER.exception(e)
            await asyncio.sleep(ERROR_INTERVAL)


async def _get_tender_contracts(tender_to_sync: dict) -> list:
    contracts = []
    if "contracts" not in tender_to_sync:
        LOGGER.warning(
            f"No contracts found in tender {tender_to_sync['id']}",
            extra=journal_context(
                {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                {"TENDER_ID": tender_to_sync["id"]}
            )
        )
        return []

    for contract in tender_to_sync.get("contracts", []):
        if contract["status"] == "active":
            if await cache_db.has(contract["id"]):
                LOGGER.info(
                    f"Contract {contract['id']} exists in local db",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_CACHED},
                        params={"CONTRACT_ID": contract["id"]}
                    ),
                )
                await cache_db.put_tender_in_cache_by_contract(contract, tender_to_sync["dateModified"])
                continue

            response = await SESSION.get(f"{BASE_URL}/contracts/{contract['id']}")
            if response.status == 404:
                LOGGER.info(
                    f"Sync contract {contract['id']} of tender {tender_to_sync['id']}",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_CONTRACT_TO_SYNC},
                        {"CONTRACT_ID": contract["id"], "TENDER_ID": tender_to_sync["id"]},
                    ),
                )
            elif response.status == 410:
                LOGGER.info(
                    f"Sync contract {contract['id']} of tender {tender_to_sync['id']} has been archived",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_CONTRACT_TO_SYNC},
                        {"CONTRACT_ID": contract["id"], "TENDER_ID": tender_to_sync["id"]},
                    ),
                )
                continue
            elif response.status != 200:
                LOGGER.warning(
                    f"Fail to contract existance {contract['id']}",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                        params={"TENDER_ID": tender_to_sync["id"], "CONTRACT_ID": contract["id"]},
                    ),
                )
                data = await response.text()
                LOGGER.exception(data)
                raise ConnectionError(f"Tender {tender_to_sync['id']} should be resynced")
            else:
                await cache_db.put(contract["id"], True)
                LOGGER.info(
                    f"Contract exists {contract['id']}",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_CONTRACT_EXISTS},
                        {"TENDER_ID": tender_to_sync["id"], "CONTRACT_ID": contract["id"]},
                    ),
                )
                await cache_db.put_tender_in_cache_by_contract(contract, tender_to_sync["dateModified"])
                continue

            contracts.append(contract)
    return contracts


async def get_tender_contracts(tender_to_sync: dict) -> list:
    while True:
        try:
            return await _get_tender_contracts(tender_to_sync)
        except Exception as e:
            LOGGER.warn(
                "Fail to handle tender contracts",
                extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION})
                )
            LOGGER.exception(e)
            await asyncio.sleep(ERROR_INTERVAL)


async def prepare_contract_data(contract: dict, credentials: dict = None) -> None:
    if not credentials:
        credentials = await get_tender_credentials(contract["tender_id"])
    data = credentials["data"]
    contract["owner"] = data["owner"]
    contract["tender_token"] = data["tender_token"]


async def put_contract(contract: dict, dateModified: str) -> None:
    while True:
        try:
            LOGGER.info(
                f"Creating contract {contract['id']} of tender {contract['tender_id']}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_CREATE_CONTRACT},
                    {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                ),
            )
            response = await SESSION.post(
                f"{BASE_URL}/contracts",
                json={"data": contract},
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {API_TOKEN}",
                },
            )
            data = await response.text()
            if response.status == 422:
                raise ValueError(data)
            elif response.status == (403, 410, 404, 405):
                raise PermissionError(data)
            elif response.status != 201:
                raise ConnectionError(data)

            LOGGER.info(
                f"Successfully created contract {contract['id']} of tender {contract['tender_id']}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_CONTRACT_CREATED},
                    {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                ),
            )
            await cache_db.put(contract["id"], True)
            await cache_db.put_tender_in_cache_by_contract(contract["tender_id"], dateModified)
            break
        except Exception as e:
            LOGGER.info(
                f"Unsuccessful put for contract {contract['id']} of tender {contract['tender_id']}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                    {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                ),
            )
            LOGGER.exception(e)
            await asyncio.sleep(ERROR_INTERVAL)


async def process_listing(server_id_cookie: str, tender: dict) -> None:
    SESSION.cookie_jar.update_cookies({"SERVER_ID": server_id_cookie})

    if not check_tender(tender):
        return None
    await cache_db.get_tender_contracts_fb(tender)
    tender_to_sync = await get_tender(tender["id"])
    contracts = await get_tender_contracts(tender_to_sync)

    for contract in contracts:
        extend_contract(contract, tender_to_sync)
        await prepare_contract_data(contract)
        await put_contract(contract, tender_to_sync["dateModified"])

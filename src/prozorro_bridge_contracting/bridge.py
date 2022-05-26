from aiohttp import ClientSession
import asyncio
import json

from prozorro_crawler.storage import get_feed_position
from prozorro_bridge_contracting.db import Db
from prozorro_bridge_contracting.settings import BASE_URL, LOGGER, ERROR_INTERVAL, HEADERS
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


async def sync_single_tender(tender_id: str, session: ClientSession = None) -> None:
    if not session:
        session = ClientSession(headers=HEADERS)
        feed_position = await get_feed_position()
        server_id = feed_position.get("server_id") if feed_position else None
        session.cookie_jar.update_cookies({"SERVER_ID": server_id})

    transferred_contracts = []
    try:
        LOGGER.info(f"Getting tender {tender_id}")
        tender = await get_tender(tender_id, session)
        LOGGER.info(f"Got tender {tender['id']} in status {tender['status']}")

        LOGGER.info(f"Getting tender {tender_id} credentials")
        tender_credentials = await get_tender_credentials(tender_id, session)
        LOGGER.info(f"Got tender {tender['id']} credentials")

        for contract in tender.get("contracts", []):
            if contract["status"] != "active":
                LOGGER.info(f"Skip contract {contract['id']} in status {contract['status']}")
                continue

            LOGGER.info(f"Checking if contract {contract['id']} already exists")
            response = await session.get(f"{BASE_URL}/contracts/{contract['id']}")
            if response.status == 200:
                LOGGER.info(f"Contract exists {contract['id']}")
                continue
            LOGGER.info(f"Contract {contract['id']} does not exists. Prepare contract for creation.")

            LOGGER.info(f"Extending contract {contract['id']} with extra data")
            extend_contract(contract, tender)
            await prepare_contract_data(contract, session, tender_credentials)

            LOGGER.info(f"Creating contract {contract['id']}")
            response = await session.post(f"{BASE_URL}/contracts/{contract['id']}", json={"data": contract})
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


async def get_tender_credentials(tender_id: str, session: ClientSession) -> dict:
    url = f"{BASE_URL}/tenders/{tender_id}/extract_credentials"
    while True:
        LOGGER.info(
            f"Getting credentials for tender {tender_id}",
            extra=journal_context(
                {"MESSAGE_ID": DATABRIDGE_GET_CREDENTIALS},
                {"TENDER_ID": tender_id}
            ),
        )
        try:
            response = await session.get(url, headers=HEADERS)
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
            raise ConnectionError(data)
        except Exception as e:
            LOGGER.warning(
                f"Can't get tender credentials {tender_id}. Exception: {type(e)} {e}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                    {"TENDER_ID": tender_id}
                ),
            )
            await asyncio.sleep(ERROR_INTERVAL)


async def get_tender(tender_id: str, session: ClientSession) -> dict:
    while True:
        try:
            response = await session.get(f"{BASE_URL}/tenders/{tender_id}", headers=HEADERS)
            data = await response.text()
            if response.status != 200:
                raise ConnectionError(data)
            return json.loads(data)["data"]
        except Exception as e:
            LOGGER.warning(
                f"Fail to get tender {tender_id}. Exception: {type(e)} {e}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                    params={"TENDER_ID": tender_id}
                )
            )
            await asyncio.sleep(ERROR_INTERVAL)


async def _get_tender_contracts(tender_to_sync: dict, session: ClientSession) -> list:
    contracts = []
    if "contracts" not in tender_to_sync:
        LOGGER.info(
            f"No contracts found in tender {tender_to_sync['id']}",
            extra=journal_context(
                {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                {"TENDER_ID": tender_to_sync["id"]}
            )
        )
        return []

    for contract in tender_to_sync.get("contracts", []):
        if contract["status"] != "active":
            continue

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

        response = await session.get(f"{BASE_URL}/contracts/{contract['id']}", headers=HEADERS)
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
            data = await response.text()
            LOGGER.warning(
                f"Fail to contract existence {contract['id']}. Error message: {str(data)}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                    params={"TENDER_ID": tender_to_sync["id"], "CONTRACT_ID": contract["id"]},
                ),
            )
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
            await cache_db.put_tender_in_cache_by_contract(tender_to_sync["id"], tender_to_sync["dateModified"])
            continue

        contracts.append(contract)
    return contracts


async def get_tender_contracts(tender_to_sync: dict, session: ClientSession) -> list:
    while True:
        try:
            return await _get_tender_contracts(tender_to_sync, session)
        except Exception as e:
            LOGGER.info(
                f"Fail to handle tender contracts. Exception: {type(e)} {e}",
                extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION})
                )
            await asyncio.sleep(ERROR_INTERVAL)


async def prepare_contract_data(contract: dict, session: ClientSession, credentials: dict = None) -> None:
    if not credentials:
        credentials = await get_tender_credentials(contract["tender_id"], session)
    data = credentials["data"]
    contract["owner"] = data["owner"]
    contract["tender_token"] = data["tender_token"]


async def put_contract(contract: dict, dateModified: str, session: ClientSession) -> None:
    while True:
        try:
            LOGGER.info(
                f"Creating contract {contract['id']} of tender {contract['tender_id']}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_CREATE_CONTRACT},
                    {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                ),
            )
            response = await session.post(f"{BASE_URL}/contracts", json={"data": contract}, headers=HEADERS)
            data = await response.text()
            if response.status == 422:
                LOGGER.error(
                    f"ATTENTION! Unsuccessful put for contract {contract['id']} of tender {contract['tender_id']}. "
                    f"This contract won't be processed. Response: {data}",
                )
                break
            elif response.status == 409:
                LOGGER.info(
                    f"Contract {contract['id']} of tender {contract['tender_id']} already exists in db",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_CONTRACT_EXISTS},
                        {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                    )
                )
                break
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
            LOGGER.warning(
                f"Unsuccessful put for contract {contract['id']} of tender {contract['tender_id']}. "
                f"Exception: {type(e)} {e}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                    {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                ),
            )
            await asyncio.sleep(ERROR_INTERVAL)


async def process_listing(session: ClientSession, tender: dict) -> None:
    if not check_tender(tender):
        return None
    await cache_db.get_tender_contracts_fb(tender)
    tender_to_sync = await get_tender(tender["id"], session)
    contracts = await get_tender_contracts(tender_to_sync, session)

    for contract in contracts:
        extend_contract(contract, tender_to_sync)
        await prepare_contract_data(contract, session)
        await put_contract(contract, tender_to_sync["dateModified"], session)

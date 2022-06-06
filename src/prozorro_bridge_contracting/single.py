import asyncio
import json

from aiohttp import ClientSession
from prozorro_crawler.storage import get_feed_position

from prozorro_bridge_contracting.bridge import (
    BASE_URL,
    HEADERS,
    get_tender_credentials,
    prepare_contract_data,
    extend_contract,
)
from prozorro_bridge_contracting.journal_msg_ids import DATABRIDGE_EXCEPTION
from prozorro_bridge_contracting.settings import LOGGER, ERROR_INTERVAL
from prozorro_bridge_contracting.utils import journal_context


async def get_tender(tender_id: str, session: ClientSession) -> dict:
    while True:
        try:
            response = await session.get(f"{BASE_URL}/tenders/{tender_id}", headers=HEADERS)
            data = await response.text()
            if response.status != 200:
                raise ConnectionError(data)
            tender = json.loads(data)["data"]
            LOGGER.debug(f"Got tender {tender_id} from api: {repr(tender)}")
            return tender
        except Exception as e:
            LOGGER.warning(
                f"Fail to get tender {tender_id}. Exception: {type(e)} {e}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                    params={"TENDER_ID": tender_id}
                )
            )
            await asyncio.sleep(ERROR_INTERVAL)


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
            LOGGER.info(f"Successfully transferred contracts: {transferred_contracts}")
        else:
            LOGGER.info(f"Tender {tender_id} does not contain contracts to transfer")
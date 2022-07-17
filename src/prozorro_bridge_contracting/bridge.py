from aiohttp import ClientSession
import asyncio
import json

from prozorro_crawler.settings import CRAWLER_USER_AGENT, API_VERSION
from prozorro_bridge_contracting.settings import LOGGER, ERROR_INTERVAL, API_TOKEN, API_HOST
from prozorro_bridge_contracting.utils import journal_context
from prozorro_bridge_contracting.journal_msg_ids import (
    DATABRIDGE_EXCEPTION,
    DATABRIDGE_GET_CREDENTIALS,
    DATABRIDGE_GOT_CREDENTIALS,
    DATABRIDGE_CONTRACT_TO_SYNC,
    DATABRIDGE_CONTRACT_EXISTS,
    DATABRIDGE_CREATE_CONTRACT,
    DATABRIDGE_CONTRACT_CREATED,
    DATABRIDGE_INFO,
    DATABRIDGE_FOUND_ACTIVE_CONTRACTS,
    DATABRIDGE_DATE_MISMATCH,
)


BASE_URL = f"{API_HOST}/api/{API_VERSION}"

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_TOKEN}",
    "User-Agent": CRAWLER_USER_AGENT,
}


def check_tender(tender: dict) -> bool:
    LOGGER.debug(f"Checking tender from feed: {repr(tender)}")

    if tender["procurementMethodType"] in ("competitiveDialogueUA", "competitiveDialogueEU", "esco"):
        return False

    if any(contract["status"] == "active" for contract in tender.get("contracts", [])):
        LOGGER.info(
            f"Found tender {tender['id']} with active contracts",
            extra=journal_context(
                {"MESSAGE_ID": DATABRIDGE_FOUND_ACTIVE_CONTRACTS},
                {"TENDER_ID": tender["id"]}
            ),
        )
    else:
        LOGGER.debug(
            f"Skipping tender {tender['id']} with no active contracts",
            extra=journal_context(
                {"MESSAGE_ID": DATABRIDGE_INFO},
                params={"TENDER_ID": tender["id"]}
            ),
        )
        return False

    return True


def extend_contract(contract: dict, tender: dict) -> None:
    contract["tender_id"] = tender["id"]
    contract["procuringEntity"] = tender["procuringEntity"]
    if tender.get("mode"):
        contract["mode"] = tender["mode"]

    # Fix deliveryDate
    for item in contract.get("items", []):
        if "deliveryDate" in item and item["deliveryDate"].get("startDate") and item["deliveryDate"].get("endDate"):
            if item["deliveryDate"]["startDate"] > item["deliveryDate"]["endDate"]:
                journal_params = {"CONTRACT_ID": contract["id"], "TENDER_ID": tender["id"]}
                LOGGER.info(
                    f"Found dates mismatch "
                    f"{item['deliveryDate']['startDate']} and {item['deliveryDate']['endDate']}",
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_DATE_MISMATCH}, journal_params),
                )
                del item["deliveryDate"]["startDate"]
                LOGGER.info(
                    "startDate value cleaned.",
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_DATE_MISMATCH}, journal_params),
                )


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


async def process_tender_contracts(tender: dict, session: ClientSession) -> list:
    while True:
        try:
            for contract in tender.get("contracts", []):
                if contract["status"] != "active":
                    LOGGER.debug(
                        f"Skipping contract {contract['id']} of tender {tender['id']} in status {contract['status']}",
                        extra=journal_context(
                            {"MESSAGE_ID": DATABRIDGE_INFO},
                            params={
                                "TENDER_ID": tender["id"],
                                "CONTRACT_ID": contract["id"],
                            }
                        ),
                    )
                    continue

                response = await session.head(f"{BASE_URL}/contracts/{contract['id']}", headers=HEADERS)
                if response.status == 404:
                    LOGGER.info(
                        f"Sync contract {contract['id']} of tender {tender['id']}",
                        extra=journal_context(
                            {"MESSAGE_ID": DATABRIDGE_CONTRACT_TO_SYNC},
                            {"CONTRACT_ID": contract["id"], "TENDER_ID": tender["id"]},
                        ),
                    )
                    extend_contract(contract, tender)
                    await prepare_contract_data(contract, session)
                    await post_contract(contract, session)
                elif response.status != 200:
                    data = await response.text()
                    LOGGER.warning(
                        f"Fail to contract existence {contract['id']}. Error message: {str(data)}",
                        extra=journal_context(
                            {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                            params={"TENDER_ID": tender["id"], "CONTRACT_ID": contract["id"]},
                        ),
                    )
                    raise ConnectionError(f"Tender {tender['id']} should be resynced")
                else:
                    LOGGER.info(
                        f"Contract exists {contract['id']}",
                        extra=journal_context(
                            {"MESSAGE_ID": DATABRIDGE_CONTRACT_EXISTS},
                            {"TENDER_ID": tender["id"], "CONTRACT_ID": contract["id"]},
                        ),
                    )
                    continue
            break
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


async def post_contract(contract: dict, session: ClientSession) -> None:
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
            if response.status == 422:
                data = await response.text()
                LOGGER.error(
                    f"ATTENTION! Unsuccessful put for contract {contract['id']} of tender {contract['tender_id']}. "
                    f"This contract won't be processed. Response: {data}",
                )
                break
            elif (
                response.status == 409 or
                response.status == 400  # FIXME: should it be changed in cdb to back to 409?
            ):
                LOGGER.info(
                    f"Contract {contract['id']} of tender {contract['tender_id']} already exists in db",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_CONTRACT_EXISTS},
                        {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                    )
                )
                break
            elif response.status == (403, 410, 404, 405):
                data = await response.text()
                raise PermissionError(data)
            elif response.status != 201:
                data = await response.text()
                raise ConnectionError(data)

            LOGGER.info(
                f"Successfully created contract {contract['id']} of tender {contract['tender_id']}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_CONTRACT_CREATED},
                    {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                ),
            )
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
    if check_tender(tender):
        await process_tender_contracts(tender, session)

# TODO: add headers and auth in requests
# TODO: fix gevent.sleep ?
# TODO: make cache_db awaitable ?

from queue import Queue

from .journal_msg_ids import *
from .utils import journal_context, generate_req_id, get_contract_award
from .db import Db
from .settings import BASE_URL, LOGGER, API_TOKEN
from aiohttp import ClientSession
import json


INFINITY_LOOP = True
config = {}
tenders_queue = Queue(maxsize=config.get("main", {}).get("buffers_size", 500))
handicap_contracts_queue = Queue(maxsize=config.get("main", {}).get("buffers_size", 500))
handicap_contracts_queue_retry = Queue(maxsize=config.get("main", {}).get("buffers_size", 500))
contracts_put_queue = Queue(maxsize=config.get("main", {}).get("buffers_size", 500))
contracts_retry_put_queue = Queue(maxsize=config.get("main", {}).get("buffers_size", 500))
cache_db = Db({})
SESSION = ClientSession()


async def get_tender_credentials(tender_id: str) -> dict:
    # TODO: add retries and done
    LOGGER.info(
        "Getting credentials for tender {}".format(tender_id),
        extra=journal_context({"MESSAGE_ID": DATABRIDGE_GET_CREDENTIALS}, {"TENDER_ID": tender_id}),
    )
    url = f"{BASE_URL}/{tender_id}/extract_credentials"
    response = await SESSION.get(
        url,
        headers={"X-Client-Request-ID": generate_req_id()}
    )
    data = await response.text()
    if response.status == 200:
        data = json.loads(data)
    else:
        # TODO: fix it
        raise Exception("failed to get credentials")
    return data


async def get_tenders(tender: dict) -> bool:
    if tender.get("procurementMethodType") in ["competitiveDialogueUA", "competitiveDialogueEU", "esco"]:
        LOGGER.info(
            "Skipping {} tender {}".format(tender["procurementMethodType"], tender["id"]),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_INFO}, params={"TENDER_ID": tender["id"]}),
        )
        return False

    if tender["status"] in ("active.qualification", "active", "active.awarded", "complete"):
        if hasattr(tender, "lots"):
            if any([1 for lot in tender["lots"] if lot["status"] == "complete"]):
                LOGGER.info(
                    "Found multilot tender {} in status {}".format(
                        tender["id"], tender["status"]
                    ),
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_FOUND_MULTILOT_COMPLETE}, {"TENDER_ID": tender["id"]}
                    ),
                )
                return True
        elif tender["status"] == "complete":
            LOGGER.info(
                "Found tender in complete status {}".format(tender["id"]),
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_FOUND_NOLOT_COMPLETE}, {"TENDER_ID": tender["id"]}
                ),
            )
            return True
    else:
        LOGGER.debug(
            "Skipping tender {} in status {}".format(
                tender["id"], tender["status"]
            ),
            extra=journal_context(params={"TENDER_ID": tender["id"]}),
        )


async def get_tender_contracts_fb(tender: dict) -> bool:
    stored = cache_db.get(tender["id"])
    if stored and stored == tender["dateModified"]:
        LOGGER.info(
            "Tender {} not modified from last check. Skipping".format(tender["id"]),
            extra=journal_context(
                {"MESSAGE_ID": DATABRIDGE_SKIP_NOT_MODIFIED}, {"TENDER_ID": tender["id"]}
            ),
        )
        return False
    LOGGER.info(
        "Backward sync: Put tender {} to process...".format(tender["id"]),
        extra=journal_context({"MESSAGE_ID": DATABRIDGE_TENDER_PROCESS}, {"TENDER_ID": tender["id"]}),
    )
    tenders_queue.put(tender)


async def _put_tender_in_cache_by_contract(tender_id: str, dateModified: str = None) -> None:
    if dateModified:
        cache_db.put(tender_id, dateModified)


def _extend_contract(contract: dict, tender: dict) -> None:
    contract["tender_id"] = tender["id"]
    contract["procuringEntity"] = tender["procuringEntity"]
    if tender.get("mode"):
        contract["mode"] = tender["mode"]
    journal_params = {"CONTRACT_ID": contract["id"], "TENDER_ID": tender["id"]}

    if not contract.get("items"):
        LOGGER.info(
            "Copying contract {} items".format(contract["id"]),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS}, journal_params),
        )
        if tender.get("lots"):
            award = get_contract_award(tender, contract)
            if award:
                if award.get("items"):
                    LOGGER.info(
                        "Copying items from related award {}".format(award["id"]),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS}, journal_params),
                    )
                    contract["items"] = award["items"]
                else:
                    LOGGER.info(
                        "Copying items matching related lot {}".format(award["lotID"]),
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS}, journal_params),
                    )
                    contract["items"] = [
                        item for item in tender["items"] if item.get("relatedLot") == award["lotID"]
                    ]
            else:
                LOGGER.warning(
                    "Not found related award for contact {} of tender {}".format(contract["id"], tender["id"]),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_AWARD_NOT_FOUND}, journal_params),
                )
        else:
            LOGGER.info(
                "Copying all tender {} items into contract {}".format(tender["id"], contract["id"]),
                extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS}, journal_params),
            )
            contract["items"] = tender.get("items", [])

    # Clear empty items
    if isinstance(contract.get("items"), list) and len(contract.get("items")) == 0:
        LOGGER.info(
            "Clearing 'items' key for contract with empty 'items' list",
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS}, journal_params),
        )
        del contract["items"]

    if not contract.get("items"):
        LOGGER.warning(
            "Contact {} of tender {} does not contain items info".format(contract["id"], tender["id"]),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_MISSING_CONTRACT_ITEMS}, journal_params),
        )

    # Fix deliveryDate
    for item in contract.get("items", []):
        if "deliveryDate" in item and item["deliveryDate"].get("startDate") and item["deliveryDate"].get("endDate"):
            if item["deliveryDate"]["startDate"] > item["deliveryDate"]["endDate"]:
                LOGGER.info(
                    "Found dates mismatch "
                    "{} and {}".format(item["deliveryDate"]["startDate"], item["deliveryDate"]["endDate"]),
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_DATE_MISMATCH}, journal_params),
                )
                del item["deliveryDate"]["startDate"]
                LOGGER.info(
                    "startDate value cleaned.",
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_DATE_MISMATCH}, journal_params),
                )

    # Add value if not exists
    if not contract.get("value"):
        LOGGER.info(
            "Contract {} does not have value. Extending with award data.".format(contract["id"]),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_VALUE}, journal_params),
        )
        award = get_contract_award(tender, contract)
        if award and award.get("value"):
            contract["value"] = award["value"]
        else:
            LOGGER.info(
                "No value found with related award for contract {}.".format(contract["id"]),
                extra=journal_context({"MESSAGE_ID": DATABRIDGE_AWARD_NOT_FOUND}, journal_params),
            )

    # Add suppliers if not exist
    if not contract.get("suppliers"):
        LOGGER.info(
            "Contract {} does not have suppliers. Extending with award data.".format(contract["id"]),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_SUPPLIERS}, journal_params),
        )
        award = get_contract_award(tender, contract)
        if award and award.get("suppliers"):
            contract["suppliers"] = award["suppliers"]
        else:
            LOGGER.info(
                "No suppliers found with related award for contract {}.".format(contract["id"]),
                extra=journal_context({"MESSAGE_ID": DATABRIDGE_AWARD_NOT_FOUND}, journal_params),
            )


async def sync_single_tender(tender_id: str) -> None:
    transfered_contracts = []
    response = await SESSION.get(f"{BASE_URL}/{tender_id}")
    data = await response.text()
    if response.status != 200:
        # TODO: fix it
        raise Exception("failed to get tender")
    tender = json.loads(data)
    tender_credentials = await get_tender_credentials(tender_id)
    for contract in tender.get("contracts", []):
        if contract["status"] != "active":
            LOGGER.info("Skip contract {} in status {}".format(contract["id"], contract["status"]))
            continue

        LOGGER.info("Checking if contract {} already exists".format(contract["id"]))
        response = await SESSION.get(f"{BASE_URL}/{tender_id}")
        if response.status == 200:
            LOGGER.info("Contract exists {}".format(contract["id"]))
            continue
        LOGGER.info("Contract {} does not exists. Prepare contract for creation.".format(contract["id"]))

        LOGGER.info("Extending contract {} with extra data".format(contract["id"]))
        _extend_contract(contract, tender)
        contract["owner"] = tender["owner"]
        contract["tender_token"] = tender_credentials["tender_token"]

        LOGGER.info("Creating contract {}".format(contract["id"]))
        response = await SESSION.post(
            f"{BASE_URL}/",
            json={"data": contract}
        )
        data = await response.text()
        if response.status != 201:
            # TODO: fix it
            raise Exception(data)
        else:
            response = await response.json()
        assert "data" in response
        LOGGER.info("Contract {} created".format(contract["id"]))
        transfered_contracts.append(contract["id"])

    if transfered_contracts:
        LOGGER.info("Successfully transfered contracts: {}".format(transfered_contracts))
    else:
        LOGGER.info("Tender {} does not contain contracts to transfer".format(tender_id))
        

async def _get_tender_contracts() -> None:
    try:
        tender_to_sync = tenders_queue.get()
        response = await SESSION.get(
            f"{BASE_URL}/{tender_to_sync['id']}",
            headers={"X-Client-Request-ID": generate_req_id()}
        )
        data = await response.text()
        if response.status == 200:
            tender = json.loads(data)["data"]
        else:
            # TODO: fix it
            raise Exception("failed to get tender")
    except Exception as e:
        LOGGER.warning(
            "Fail to get tender info {}".format(tender_to_sync["id"]),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION}, params={"TENDER_ID": tender_to_sync["id"]}),
        )
        LOGGER.exception(e)
        LOGGER.info(
            "Put tender {} back to tenders queue".format(tender_to_sync["id"]),
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION}, params={"TENDER_ID": tender_to_sync["id"]}),
        )
        tenders_queue.put(tender_to_sync)
        # gevent.sleep(self.on_error_delay)
    else:
        if "contracts" not in tender:
            LOGGER.warning(
                "!!!No contracts found in tender {}".format(tender["id"]),
                extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION}, params={"TENDER_ID": tender["id"]}),
            )
            return
        for contract in tender["contracts"]:
            if contract["status"] == "active":
                if not cache_db.has(contract["id"]):
                    response = await SESSION.get(f"{BASE_URL}/{tender['id']}/contracts/{contract['id']}")
                    data = await response.text()
                    if response.status == 404:
                        LOGGER.info(
                            "Sync contract {} of tender {}".format(contract["id"], tender["id"]),
                            extra=journal_context(
                                {"MESSAGE_ID": DATABRIDGE_CONTRACT_TO_SYNC},
                                {"CONTRACT_ID": contract["id"], "TENDER_ID": tender["id"]},
                            ),
                        )
                    elif response.status == 410:
                        LOGGER.info(
                            "Sync contract {} of tender {} has been archived".format(contract["id"], tender["id"]),
                            extra=journal_context(
                                {"MESSAGE_ID": DATABRIDGE_CONTRACT_TO_SYNC},
                                {"CONTRACT_ID": contract["id"], "TENDER_ID": tender["id"]},
                            ),
                        )
                        continue
                    elif response.status != 200:
                        LOGGER.warning(
                            "Fail to contract existance {}".format(contract["id"]),
                            extra=journal_context(
                                {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                params={"TENDER_ID": tender_to_sync["id"], "CONTRACT_ID": contract["id"]},
                            ),
                        )
                        LOGGER.exception(data)
                        LOGGER.info(
                            "Put tender {} back to tenders queue".format(tender_to_sync["id"]),
                            extra=journal_context(
                                {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                                params={"TENDER_ID": tender_to_sync["id"], "CONTRACT_ID": contract["id"]},
                            ),
                        )
                        tenders_queue.put(tender_to_sync)
                        raise
                    else:
                        cache_db.put(contract["id"], True)
                        LOGGER.info(
                            "Contract exists {}".format(contract["id"]),
                            extra=journal_context(
                                {"MESSAGE_ID": DATABRIDGE_CONTRACT_EXISTS},
                                {"TENDER_ID": tender_to_sync["id"], "CONTRACT_ID": contract["id"]},
                            ),
                        )
                        await _put_tender_in_cache_by_contract(contract, tender_to_sync["dateModified"])
                        continue
                else:
                    LOGGER.info(
                        "Contract {} exists in local db".format(contract["id"]),
                        extra=journal_context(
                            {"MESSAGE_ID": DATABRIDGE_CACHED}, params={"CONTRACT_ID": contract["id"]}
                        ),
                    )
                    await _put_tender_in_cache_by_contract(contract, tender_to_sync["dateModified"])
                    continue

                LOGGER.info("Extending contract {} with extra data".format(contract["id"]))
                _extend_contract(contract, tender)
                handicap_contracts_queue.put(contract)


async def get_tender_contracts() -> None:
    while True:
        try:
            await _get_tender_contracts()
        except Exception as e:
            LOGGER.warning(
                "Fail to handle tender contracts", extra=journal_context({"MESSAGE_ID": DATABRIDGE_EXCEPTION})
            )
            LOGGER.exception(e)
            # gevent.sleep(self.on_error_delay)
            raise
        # gevent.sleep(0)


async def prepare_contract_data() -> None:
    unsuccessful_contracts = set()
    unsuccessful_contracts_limit = 10
    while INFINITY_LOOP:
        contract = handicap_contracts_queue.get()
        try:
            LOGGER.info(
                "Getting extra info for tender {}".format(contract["tender_id"]),
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_GET_EXTRA_INFO},
                    {"TENDER_ID": contract["tender_id"], "CONTRACT_ID": contract["id"]},
                ),
            )
            tender_data = await get_tender_credentials(contract["tender_id"])
            assert "owner" in tender_data.get("data", {})
            assert "tender_token" in tender_data.get("data", {})
            unsuccessful_contracts.clear()
        except Exception as e:
            LOGGER.warning(
                "Can't get tender credentials {}".format(contract["tender_id"]),
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                    {"TENDER_ID": contract["tender_id"], "CONTRACT_ID": contract["id"]},
                ),
            )
            LOGGER.exception(e)
            handicap_contracts_queue_retry.put(contract)
            unsuccessful_contracts.add(contract["id"])
            if len(unsuccessful_contracts) >= unsuccessful_contracts_limit:
                # Current server stopped processing requests, reconnecting to other
                LOGGER.info(
                    "Reconnecting tenders client",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_RECONNECT},
                        {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                    ),
                )
                unsuccessful_contracts.clear()
            # gevent.sleep(self.on_error_delay)
        else:
            LOGGER.debug(
                "Got extra info for tender {}".format(contract["tender_id"]),
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_GOT_EXTRA_INFO},
                    {"TENDER_ID": contract["tender_id"], "CONTRACT_ID": contract["id"]},
                ),
            )
            data = tender_data["data"]
            contract["owner"] = data["owner"]
            contract["tender_token"] = data["tender_token"]
            contracts_put_queue.put(contract)
        # gevent.sleep(0)


async def prepare_contract_data_retry() -> None:
    while INFINITY_LOOP:
        contract = handicap_contracts_queue_retry.get()
        tender_data = await get_tender_credentials(contract)
        LOGGER.debug(
            "Got extra info for tender {}".format(contract["tender_id"]),
            extra=journal_context(
                {"MESSAGE_ID": DATABRIDGE_GOT_EXTRA_INFO},
                {"TENDER_ID": contract["tender_id"], "CONTRACT_ID": contract["id"]},
            ),
        )
        data = tender_data["data"]
        contract["owner"] = data["owner"]
        contract["tender_token"] = data["tender_token"]
        contracts_put_queue.put(contract)
        # gevent.sleep(0)


async def put_contracts() -> None:
    unsuccessful_contracts = set()
    unsuccessful_contracts_limit = 10
    while INFINITY_LOOP:
        contract = contracts_put_queue.get()
        LOGGER.info(
            "Creating contract {} of tender {}".format(contract["id"], contract["tender_id"]),
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
                "Authorization": f"Basic {API_TOKEN}",
            },
        )
        data = await response.text()
        if response.status != 201:
            LOGGER.info(
                "Unsuccessful put for contract {0} of tender {1}".format(contract["id"], contract["tender_id"]),
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                    {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                ),
            )
            LOGGER.exception(data)
            LOGGER.info(
                "Schedule retry for contract {0}".format(contract["id"]),
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_RETRY_CREATE},
                    {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                ),
            )
            contracts_retry_put_queue.put(contract)
            unsuccessful_contracts.add(contract["id"])
            if len(unsuccessful_contracts) >= unsuccessful_contracts_limit:
                # Current server stopped processing requests, reconnecting to other
                LOGGER.info(
                    "Reconnecting contract client",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_RECONNECT},
                        {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                    ),
                )
                unsuccessful_contracts.clear()
        else:
            unsuccessful_contracts.clear()
            LOGGER.info(
                "Successfully created contract {} of tender {}".format(contract["id"], contract["tender_id"]),
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_CONTRACT_CREATED},
                    {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                ),
            )
            cache_db.put(contract["id"], True)
            await _put_tender_in_cache_by_contract(contract, contract["tender_id"])
        # gevent.sleep(0)


async def _put_with_retry(contract: dict) -> None:
    try:
        LOGGER.info(
            "Creating contract {} of tender {}".format(contract["id"], contract["tender_id"]),
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
                "Authorization": f"Basic {API_TOKEN}",
            },
        )
        data = await response.text()
        if response.status != 201:
            raise Exception(data)
    except Exception as e:
        LOGGER.exception(e)
        raise


async def retry_put_contracts() -> None:
    while INFINITY_LOOP:
        try:
            contract = contracts_retry_put_queue.get()
            await _put_with_retry(contract)
            LOGGER.info(
                "Successfully created contract {} of tender {}".format(contract["id"], contract["tender_id"]),
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_CONTRACT_CREATED},
                    {"CONTRACT_ID": contract["id"], "TENDER_ID": contract["tender_id"]},
                ),
            )
        except Exception as e:
            LOGGER.warning(
                "Can't create contract {}".format(contract["id"]),
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
                    {"TENDER_ID": contract["tender_id"], "CONTRACT_ID": contract["id"]},
                ),
            )
        else:
            cache_db.put(contract["id"], True)
            await _put_tender_in_cache_by_contract(contract, contract["tender_id"])
        # gevent.sleep(0)


async def process_listing(server_id_cookie: str, tender: dict) -> None:
    if not await get_tenders(tender):
        return None
    await get_tender_contracts_fb(tender)

from prozorro_bridge_contracting.settings import LOGGER
from prozorro_bridge_contracting.journal_msg_ids import (
    DATABRIDGE_FOUND_MULTILOT_COMPLETE,
    DATABRIDGE_FOUND_NOLOT_COMPLETE,
    DATABRIDGE_COPY_CONTRACT_ITEMS,
    DATABRIDGE_MISSING_CONTRACT_ITEMS,
    DATABRIDGE_COPY_CONTRACT_SUPPLIERS,
    DATABRIDGE_COPY_CONTRACT_VALUE,
    DATABRIDGE_DATE_MISMATCH,
    DATABRIDGE_AWARD_NOT_FOUND,
    DATABRIDGE_INFO,
)


def journal_context(record: dict = None, params: dict = None) -> dict:
    if record is None:
        record = {}
    if params is None:
        params = {}
    for k, v in params.items():
        record["JOURNAL_" + k] = v
    return record


def get_contract_award(tender: dict, contract: dict) -> dict:
    for award in tender.get("awards", []):
        if award.get("id") == contract.get("awardId"):
            return award
    return {}


def extend_contract(contract: dict, tender: dict) -> None:
    contract["tender_id"] = tender["id"]
    contract["procuringEntity"] = tender["procuringEntity"]
    if tender.get("mode"):
        contract["mode"] = tender["mode"]
    journal_params = {"CONTRACT_ID": contract["id"], "TENDER_ID": tender["id"]}
    award = get_contract_award(tender, contract)

    if not contract.get("items"):
        LOGGER.info(
            f"Copying contract {contract['id']} items",
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS}, journal_params),
        )
        if tender.get("lots"):
            if award:
                if award.get("items"):
                    LOGGER.info(
                        f"Copying items from related award {award['id']}",
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS}, journal_params),
                    )
                    contract["items"] = award["items"]
                else:
                    LOGGER.info(
                        f"Copying items matching related lot {award['lotID']}",
                        extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS}, journal_params),
                        )
                    contract["items"] = [
                        item for item in tender["items"] if item.get("relatedLot") == award["lotID"]
                    ]
            else:
                LOGGER.warning(
                    f"Not found related award for contact {contract['id']} of tender {tender['id']}",
                    extra=journal_context({"MESSAGE_ID": DATABRIDGE_AWARD_NOT_FOUND}, journal_params),
                )
        else:
            LOGGER.info(
                f"Copying all tender {tender['id']} items into contract {contract['id']}",
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
            f"Contact {contract['id']} of tender {tender['id']} does not contain items info",
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_MISSING_CONTRACT_ITEMS}, journal_params),
        )

    # Fix deliveryDate
    for item in contract.get("items", []):
        if "deliveryDate" in item and item["deliveryDate"].get("startDate") and item["deliveryDate"].get("endDate"):
            if item["deliveryDate"]["startDate"] > item["deliveryDate"]["endDate"]:
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

    # Add value if not exists
    if not contract.get("value"):
        LOGGER.info(
            f"Contract {contract['id']} does not have value. Extending with award data.",
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_VALUE}, journal_params),
        )
        if award and award.get("value"):
            contract["value"] = award["value"]
        else:
            LOGGER.info(
                f"No value found with related award for contract {contract['id']}.",
                extra=journal_context({"MESSAGE_ID": DATABRIDGE_AWARD_NOT_FOUND}, journal_params),
            )

    # Add suppliers if not exist
    if not contract.get("suppliers"):
        LOGGER.info(
            f"Contract {contract['id']} does not have suppliers. Extending with award data.",
            extra=journal_context({"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_SUPPLIERS}, journal_params),
        )
        if award and award.get("suppliers"):
            contract["suppliers"] = award["suppliers"]
        else:
            LOGGER.info(
                f"No suppliers found with related award for contract {contract['id']}.",
                extra=journal_context({"MESSAGE_ID": DATABRIDGE_AWARD_NOT_FOUND}, journal_params),
            )


def check_tender(tender: dict) -> bool:
    if (
            tender["status"] in ("active.qualification", "active", "active.awarded", "complete")
            and tender["procurementMethodType"] not in ("competitiveDialogueUA", "competitiveDialogueEU", "esco")
    ):
        if "lots" in tender:
            if any(lot["status"] == "complete" for lot in tender["lots"]):
                LOGGER.info(
                    f"Found multilot tender {tender['id']} in status {tender['status']}",
                    extra=journal_context(
                        {"MESSAGE_ID": DATABRIDGE_FOUND_MULTILOT_COMPLETE}, 
                        {"TENDER_ID": tender["id"]}
                    ),
                )
                return True
        elif tender["status"] == "complete":
            LOGGER.info(
                f"Found tender in complete status {tender['id']}",
                extra=journal_context(
                    {"MESSAGE_ID": DATABRIDGE_FOUND_NOLOT_COMPLETE}, 
                    {"TENDER_ID": tender["id"]}
                ),
            )
            return True
    LOGGER.debug(
        f"Skipping tender {tender['id']} in status {tender['status']}",
        extra=journal_context(
            {"MESSAGE_ID": DATABRIDGE_INFO},
            params={"TENDER_ID": tender["id"]}
        ),
    )
    return False

# TODO: in get_contract_award no needed full contract just awardId

from uuid import uuid4


def journal_context(record=None, params=None):
    if record is None:
        record = {}
    if params is None:
        params = {}
    for k, v in params.items():
        record["JOURNAL_" + k] = v
    return record


def generate_req_id():
    return b"contracting-data-bridge-req-" + str(uuid4()).encode("ascii")


def get_contract_award(tender: dict, contract: dict) -> dict:
    for award in tender.get("awards", []):
        if award.get("id") == contract.get("awardId"):
            return award
    return {}


# Done
def extend_contract(contract: dict, tender: dict) -> None:
    contract["tender_id"] = tender["id"]
    contract["procuringEntity"] = tender["procuringEntity"]
    if tender.get("mode"):
        contract["mode"] = tender["mode"]
    journal_params = {"CONTRACT_ID": contract["id"], "TENDER_ID": tender["id"]}

    if not contract.get("items"):
        if tender.get("lots"):
            award = get_contract_award(tender, contract)
            if award:
                if award.get("items"):
                    contract["items"] = award["items"]
                else:
                    contract["items"] = [
                        item for item in tender["items"] if item.get("relatedLot") == award["lotID"]
                    ]
            else:
                pass
        else:
            contract["items"] = tender.get("items", [])

    # Clear empty items
    if isinstance(contract.get("items"), list) and len(contract.get("items")) == 0:
        del contract["items"]

    if not contract.get("items"):
        pass

    # Fix deliveryDate
    for item in contract.get("items", []):
        if "deliveryDate" in item and item["deliveryDate"].get("startDate") and item["deliveryDate"].get("endDate"):
            if item["deliveryDate"]["startDate"] > item["deliveryDate"]["endDate"]:
                del item["deliveryDate"]["startDate"]

    # Add value if not exists
    if not contract.get("value"):
        award = get_contract_award(tender, contract)
        if award and award.get("value"):
            contract["value"] = award["value"]
        else:
            pass

    # Add suppliers if not exist
    if not contract.get("suppliers"):
        award = get_contract_award(tender, contract)
        if award and award.get("suppliers"):
            contract["suppliers"] = award["suppliers"]
        else:
            pass

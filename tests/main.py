from copy import deepcopy
from datetime import datetime
import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock, call

from prozorro_bridge_contracting.journal_msg_ids import *
from prozorro_bridge_contracting.utils import check_tender, journal_context
from prozorro_bridge_contracting.bridge import (
    get_tender_credentials,
    get_tender,
    get_tender_contracts,
    post_contract,
    sync_single_tender,
    prepare_contract_data,
    process_listing, 
    HEADERS,
    BASE_URL,
)


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_get_tender_credentials(mocked_logger):
    tender_data = {"id": "42"}
    session_mock = AsyncMock()
    error_data = {"error": "No permission"}
    session_mock.get = AsyncMock(side_effect=[
        MagicMock(status=403, text=AsyncMock(return_value=error_data)),
        MagicMock(status=200, text=AsyncMock(return_value=json.dumps(tender_data))),
    ])

    with patch("prozorro_bridge_contracting.bridge.asyncio.sleep", AsyncMock()) as mocked_sleep:
        data = await get_tender_credentials(tender_data["id"], session_mock)

    assert session_mock.get.await_count == 2
    assert data == tender_data
    assert mocked_logger.warning.call_count == 1
    isinstance(mocked_logger.warning.call_args.args[0], ConnectionError)
    assert mocked_sleep.await_count == 1


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
@patch("prozorro_bridge_contracting.bridge.get_feed_position", AsyncMock(return_value={}))
async def test_sync_single_tender_contract_skip(mocked_logger):
    contract_data = {"status": "no_active", "id": "1"}
    tender_data = {
        "id": "33",
        "status": "active",
        "procuringEntity": "procuringEntity",
        "owner": "owner",
        "tender_token": "tender_token",
        "contracts": [contract_data],
    }
    session_mock = AsyncMock()
    session_mock.cookie_jar = MagicMock()
    session_mock.get = AsyncMock(
        return_value=MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_data})))
    )

    await sync_single_tender(tender_data["id"], session_mock)

    assert session_mock.get.await_count == 2
    calls = [
        call(f"Getting tender {tender_data['id']}"),
        call(f"Got tender {tender_data['id']} in status active"),
        call(f"Getting tender {tender_data['id']} credentials"),
        call(
            f"Getting credentials for tender {tender_data['id']}",
            extra={"MESSAGE_ID": DATABRIDGE_GET_CREDENTIALS, "JOURNAL_TENDER_ID": tender_data['id']}
        ),
        call(
            f"Got tender {tender_data['id']} credentials",
            extra={"MESSAGE_ID": DATABRIDGE_GOT_CREDENTIALS, "JOURNAL_TENDER_ID": tender_data['id']}
        ),
        call(f"Got tender {tender_data['id']} credentials"),
        call(f"Skip contract {contract_data['id']} in status no_active"),
        call(f"Tender {tender_data['id']} does not contain contracts to transfer"),
    ]
    assert mocked_logger.info.call_args_list == calls
    assert session_mock.post.await_count == 0


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.get_feed_position", AsyncMock(return_value={}))
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_sync_single_tender_contract_skip_exists(mocked_logger):
    contract_data = {"status": "active", "id": "1"}
    tender_data = {
        "id": "33",
        "status": "active",
        "procuringEntity": "procuringEntity",
        "owner": "owner",
        "tender_token": "tender_token",
        "contracts": [contract_data],
    }
    session_mock = AsyncMock()
    session_mock.cookie_jar = MagicMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_data}))),
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_data}))),
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": contract_data}))),
        ]
    )

    await sync_single_tender(tender_data["id"], session_mock)

    assert session_mock.get.await_count == 3
    calls = [
        call(f"Getting tender {tender_data['id']}"),
        call(f"Got tender {tender_data['id']} in status active"),
        call(f"Getting tender {tender_data['id']} credentials"),
        call(
            f"Getting credentials for tender {tender_data['id']}",
            extra={"MESSAGE_ID": DATABRIDGE_GET_CREDENTIALS, "JOURNAL_TENDER_ID": tender_data['id']}
        ),
        call(
            f"Got tender {tender_data['id']} credentials",
            extra={"MESSAGE_ID": DATABRIDGE_GOT_CREDENTIALS, "JOURNAL_TENDER_ID": tender_data['id']}
        ),
        call(f"Got tender {tender_data['id']} credentials"),
        call(f"Checking if contract {contract_data['id']} already exists"),
        call(f"Contract exists {contract_data['id']}"),
        call(f"Tender {tender_data['id']} does not contain contracts to transfer"),
    ]
    assert mocked_logger.info.call_args_list == calls
    assert session_mock.post.await_count == 0


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
@patch("prozorro_bridge_contracting.bridge.get_feed_position", AsyncMock(return_value={}))
async def test_sync_single_tender(mocked_logger):
    tender_credentials_data = {
        "procuringEntity": "procuringEntity",
        "tender_token": "tender_token",
        "owner": "owner",
    }
    value_data = {"amount": 1, "currency": "UAH", "valueAddedTaxIncluded": True}
    contract_data = {
        "status": "active",
        "id": 1,
        "value": value_data,
        "items": [{}],
        "suppliers": [{}],
    }
    tender_data = {
        "id": "33",
        "status": "active",
        "procuringEntity": "procuringEntity",
        "owner": "owner",
        "tender_token": "tender_token",
        "contracts": [contract_data],
    }

    session_mock = AsyncMock()
    session_mock.cookie_jar = MagicMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_data}))),
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_credentials_data}))),
            MagicMock(status=404),
        ]
    )
    session_mock.post = AsyncMock(
        return_value=MagicMock(
            status=201,
            json=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
            text=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
        )
    )
    await sync_single_tender(tender_data["id"], session_mock)

    calls = [
        call(f"Checking if contract {contract_data['id']} already exists"),
        call(f"Contract {contract_data['id']} does not exists. Prepare contract for creation."),
        call(f"Extending contract {contract_data['id']} with extra data"),
        call(f"Creating contract {contract_data['id']}"),
        call(f"Contract {contract_data['id']} created"),
        call(f"Successfully transfered contracts: [{contract_data['id']}]")
    ]
    assert calls in mocked_logger.info.call_args_list
    assert session_mock.post.await_count == 1
    assert session_mock.post.await_args[1]["json"]["data"]["id"] == contract_data["id"]
    assert session_mock.post.await_args[1]["json"]["data"]["status"] == contract_data["status"]
    assert session_mock.post.await_args[1]["json"]["data"]["value"] == contract_data["value"]


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.utils.LOGGER")
@patch("prozorro_bridge_contracting.bridge.LOGGER")
@patch("prozorro_bridge_contracting.bridge.get_feed_position", AsyncMock(return_value={}))
async def test_sync_single_tender_without_extra_data_and_awards(mocked_logger, mocked_utils_logger):
    tender_credentials_data = {
        "procuringEntity": "procuringEntity",
        "tender_token": "tender_token",
        "owner": "owner"
    }
    contract_data = {"status": "active", "id": 1}
    tender_data = {
        "id": "33",
        "status": "active",
        "procuringEntity": "procuringEntity",
        "owner": "owner",
        "tender_token": "tender_token",
        "contracts": [contract_data],
        "items": [{}],
        "suppliers": [{}],
    }

    session_mock = AsyncMock()
    session_mock.cookie_jar = MagicMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_data}))),
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_credentials_data}))),
            MagicMock(status=404),
        ]
    )
    session_mock.post = AsyncMock(
        return_value=MagicMock(
            status=201,
            json=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
            text=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
        )
    )
    await sync_single_tender(tender_data["id"], session_mock)

    calls = [
        call(f"Contract {contract_data['id']} does not exists. Prepare contract for creation."),
        call(f"Extending contract {contract_data['id']} with extra data"),
        call(f"Creating contract {contract_data['id']}"),
        call(f"Contract {contract_data['id']} created"),
        call(f"Successfully transfered contracts: [{contract_data['id']}]"),
    ]
    assert calls in mocked_logger.info.call_args_list

    extra = {"JOURNAL_CONTRACT_ID": contract_data["id"], "JOURNAL_TENDER_ID": tender_data["id"]}
    utils_calls = [
        call(
            f"Copying contract {contract_data['id']} items",
            extra={"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS, **extra}
        ),
        call(
            f"Copying all tender {tender_data['id']} items into contract {contract_data['id']}",
            extra={"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS, **extra}
        ),
        call(
            f"Contract {contract_data['id']} does not have value. Extending with award data.",
            extra={"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_VALUE, **extra}
        ),
        call(
            f"No value found with related award for contract {contract_data['id']}.",
            extra={"MESSAGE_ID": DATABRIDGE_AWARD_NOT_FOUND, **extra}
        ),
        call(
            f"Contract {contract_data['id']} does not have suppliers. Extending with award data.",
            extra={"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_SUPPLIERS, **extra}
        ),
        call(
            f"No suppliers found with related award for contract {contract_data['id']}.",
            extra={"MESSAGE_ID": DATABRIDGE_AWARD_NOT_FOUND, **extra}
        )
    ]
    assert mocked_utils_logger.info.call_args_list == utils_calls
    assert session_mock.post.await_count == 1
    assert session_mock.post.await_args[1]["json"]["data"]["id"] == contract_data["id"]
    assert session_mock.post.await_args[1]["json"]["data"]["status"] == contract_data["status"]
    assert "value" not in session_mock.post.await_args[1]["json"]["data"]


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.utils.LOGGER")
@patch("prozorro_bridge_contracting.bridge.LOGGER")
@patch("prozorro_bridge_contracting.bridge.get_feed_position", AsyncMock(return_value={}))
async def test_sync_single_tender_with_lots(mocked_logger, mocked_utils_logger):
    tender_credentials_data = {
        "procuringEntity": "procuringEntity",
        "tender_token": "tender_token",
        "owner": "owner"
    }
    contract_data = {"status": "active", "id": 1, "awardId": "test_award"}
    award = {"id": "test_award", "items": {"id": "test_item"}}
    tender_data = {
        "id": "33",
        "status": "active",
        "procuringEntity": "procuringEntity",
        "owner": "owner",
        "tender_token": "tender_token",
        "contracts": [contract_data],
        "awards": [award],
        "items": [{}],
        "lots": [{}],
        "suppliers": [{}],
    }

    session_mock = AsyncMock()
    session_mock.cookie_jar = MagicMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_data}))),
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_credentials_data}))),
            MagicMock(status=404),
        ]
    )
    session_mock.post = AsyncMock(
        return_value=MagicMock(
            status=201,
            json=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
            text=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
        )
    )
    await sync_single_tender(tender_data["id"], session_mock)

    calls = [
        call(f"Contract {contract_data['id']} does not exists. Prepare contract for creation."),
        call(f"Extending contract {contract_data['id']} with extra data"),
        call(f"Creating contract {contract_data['id']}"),
        call(f"Contract {contract_data['id']} created"),
        call(f"Successfully transfered contracts: [{contract_data['id']}]"),
    ]
    assert calls in mocked_logger.info.call_args_list

    extra = {"JOURNAL_CONTRACT_ID": contract_data["id"], "JOURNAL_TENDER_ID": tender_data["id"]}
    utils_calls = [
        call(
            f"Copying contract {contract_data['id']} items",
            extra={"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS, **extra}
        ),
        call(
            f"Copying items from related award {award['id']}",
            extra={"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_ITEMS, **extra}
        ),
        call(
            f"Contract {contract_data['id']} does not have value. Extending with award data.",
            extra={"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_VALUE, **extra}
        ),
        call(
            f"No value found with related award for contract {contract_data['id']}.",
            extra={"MESSAGE_ID": DATABRIDGE_AWARD_NOT_FOUND, **extra}
        ),
        call(
            f"Contract {contract_data['id']} does not have suppliers. Extending with award data.",
            extra={"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_SUPPLIERS, **extra}
        ),
        call(
            f"No suppliers found with related award for contract {contract_data['id']}.",
            extra={"MESSAGE_ID": DATABRIDGE_AWARD_NOT_FOUND, **extra}
        )
    ]
    assert mocked_utils_logger.info.call_args_list == utils_calls
    assert session_mock.post.await_count == 1
    assert session_mock.post.await_args[1]["json"]["data"]["id"] == contract_data["id"]
    assert session_mock.post.await_args[1]["json"]["data"]["status"] == contract_data["status"]
    assert "value" not in session_mock.post.await_args[1]["json"]["data"]


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.utils.LOGGER")
@patch("prozorro_bridge_contracting.bridge.LOGGER")
@patch("prozorro_bridge_contracting.bridge.get_feed_position", AsyncMock(return_value={}))
async def test_sync_single_tender_without_extra_data(mocked_logger, mocked_utils_logger):
    tender_credentials_data = {
        "procuringEntity": "procuringEntity",
        "tender_token": "tender_token",
        "owner": "owner"
    }
    value_data = {"amount": 1, "currency": "UAH", "valueAddedTaxIncluded": True}
    supplier_data = {"id": 3}
    item_data = {"id": 4}
    award_data = {
        "status": "active",
        "id": 1,
        "value": value_data,
        "suppliers": [supplier_data],
    }
    contract_data = {"status": "active", "id": 2, "awardId": 1}
    tender_data = {
        "id": "33",
        "status": "active",
        "procuringEntity": "procuringEntity",
        "owner": "owner",
        "tender_token": "tender_token",
        "contracts": [contract_data],
        "awards": [award_data],
        "items": [item_data],
    }

    session_mock = AsyncMock()
    session_mock.cookie_jar = MagicMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_data}))),
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_credentials_data}))),
            MagicMock(status=404),
        ]
    )
    session_mock.post = AsyncMock(
        return_value=MagicMock(
            status=201,
            json=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
            text=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
        )
    )
    await sync_single_tender(tender_data["id"], session_mock)

    calls = [
        call(f"Contract {contract_data['id']} does not exists. Prepare contract for creation."),
        call(f"Extending contract {contract_data['id']} with extra data"),
        call(f"Creating contract {contract_data['id']}"),
        call(f"Contract {contract_data['id']} created"),
        call(f"Successfully transfered contracts: [{contract_data['id']}]"),
    ]
    assert calls in mocked_logger.info.call_args_list

    extra = {"JOURNAL_CONTRACT_ID": contract_data["id"], "JOURNAL_TENDER_ID": tender_data["id"]}
    utils_calls = [
        call(
            f"Contract {contract_data['id']} does not have value. Extending with award data.",
            extra={"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_VALUE, **extra}
        ),
        call(
            f"Contract {contract_data['id']} does not have suppliers. Extending with award data.",
            extra={"MESSAGE_ID": DATABRIDGE_COPY_CONTRACT_SUPPLIERS, **extra}
        ),
    ]
    assert utils_calls in mocked_utils_logger.info.call_args_list
    assert session_mock.post.await_count == 1
    assert session_mock.post.await_args[1]["json"]["data"]["id"] == contract_data["id"]
    assert session_mock.post.await_args[1]["json"]["data"]["status"] == contract_data["status"]
    assert session_mock.post.await_args[1]["json"]["data"]["value"] == value_data
    assert session_mock.post.await_args[1]["json"]["data"]["suppliers"] == [supplier_data]
    assert session_mock.post.await_args[1]["json"]["data"]["items"] == [item_data]


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.utils.LOGGER", MagicMock())
@patch("prozorro_bridge_contracting.bridge.LOGGER")
@patch("prozorro_bridge_contracting.bridge.get_feed_position", AsyncMock(return_value={}))
async def test_sync_single_tender_Exception(mocked_logger):
    tender_credentials_data = {
        "procuringEntity": "procuringEntity",
        "tender_token": "tender_token",
        "owner": "owner"
    }
    contract_data = {"status": "active", "id": 1}
    tender_data = {
        "id": "33",
        "status": "active",
        "procuringEntity": "procuringEntity",
        "owner": "owner",
        "tender_token": "tender_token",
        "contracts": [contract_data],
    }

    session_mock = AsyncMock()
    session_mock.cookie_jar = MagicMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_data}))),
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_credentials_data}))),
            MagicMock(status=404),
        ]
    )
    session_mock.post = AsyncMock(
        return_value=MagicMock(
            status=201,
            json=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
            text=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
        )
    )
    await sync_single_tender(tender_data["id"], session_mock)

    assert session_mock.post.await_count == 1
    assert call(f"Successfully transfered contracts: [{contract_data['id']}]") in mocked_logger.info.call_args_list

    session_mock = AsyncMock()
    session_mock.cookie_jar = MagicMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_data}))),
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_credentials_data}))),
            Exception("Error!"),
        ]
    )
    with pytest.raises(Exception) as e:
        await sync_single_tender(tender_data["id"], session_mock)
    assert "Error!" in str(e.value)


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_retry_post_contracts_success(mocked_logger):
    contract = {"id": "42", "tender_id": "1984"}
    session_mock = AsyncMock()
    e = Exception("Test error")
    side_effect = [
        e,
        MagicMock(status=201, text=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]}))),
    ]
    session_mock.post = AsyncMock(side_effect=side_effect)

    with patch("prozorro_bridge_contracting.bridge.asyncio.sleep", AsyncMock()) as mocked_sleep:
        await post_contract(contract, session_mock)

    post_call = call(f"{BASE_URL}/contracts", json={'data': contract}, headers=HEADERS)
    session_mock.mock_calls = [post_call] * 2
    mocked_logger.warning.assert_called_once()
    mocked_sleep.assert_called_once_with(5)


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_retry_post_contracts_fail(mocked_logger):
    contract = {"id": "42", "tender_id": "1984"}
    session_mock = AsyncMock()
    e = Exception("Test error")
    side_effect = [
        e,
        MagicMock(status=422, text=AsyncMock(return_value=json.dumps({"error": "Failed"}))),
        MagicMock(status=201, text=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]}))),
    ]
    session_mock.post = AsyncMock(side_effect=side_effect)
    with patch("prozorro_bridge_contracting.bridge.asyncio.sleep", AsyncMock()) as mocked_sleep:
        await post_contract(contract, session_mock)

    post_call = call(f"{BASE_URL}/contracts", json={'data': contract}, headers=HEADERS)
    session_mock.mock_calls = [post_call] * 3

    assert mocked_logger.warning.call_count == 1
    assert mocked_sleep.call_count == 1


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_post_contracts(mocked_logger):
    list_contracts = []
    side_effect = []
    for i in range(0, 10):
        list_contracts.append({"id": str(i), "tender_id": str(i + 100)})
        side_effect.append(
            MagicMock(status=201, text=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]}))),
        )
    session_mock = AsyncMock()
    session_mock.post = AsyncMock(side_effect=side_effect)

    for contract in list_contracts:
        await post_contract(contract, session_mock)

    assert len(session_mock.post.await_args_list) == 10
    assert mocked_logger.warning.call_count == 0



@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_prepare_contract_data_retry(mocked_logger):
    contract = {"id": "42", "tender_id": "1984"}
    tender_credentials_data = {
        "procuringEntity": "procuringEntity",
        "tender_token": "tender_token",
        "owner": "owner"
    }
    test_contract = deepcopy(contract)
    test_contract["owner"] = tender_credentials_data["owner"]
    test_contract["tender_token"] = tender_credentials_data["tender_token"]
    session_mock = AsyncMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_credentials_data}))),
        ]
    )
    await prepare_contract_data(contract, session_mock)

    assert test_contract == contract
    assert mocked_logger.warning.call_count == 0


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_prepare_contract_data_retry_with_exception(mocked_logger):
    contract = {"id": "42", "tender_id": "1984"}
    tender_credentials_data = {
        "procuringEntity": "procuringEntity",
        "tender_token": "tender_token",
        "owner": "owner"
    }
    test_contract = deepcopy(contract)
    test_contract["owner"] = tender_credentials_data["owner"]
    test_contract["tender_token"] = tender_credentials_data["tender_token"]
    session_mock = AsyncMock()
    e = Exception("Test error")
    session_mock.get = AsyncMock(
        side_effect=[
            e,
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_credentials_data}))),
        ]
    )
    with patch("prozorro_bridge_contracting.bridge.asyncio.sleep", AsyncMock()) as mocked_sleep:
        await prepare_contract_data(contract, session_mock)

    mocked_logger.warning.assert_called_once()
    mocked_sleep.assert_called_once_with(5)
    assert test_contract == contract


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_prepare_contract_data_with_exception(mocked_logger):
    contract = {"id": "42", "tender_id": "1984"}
    tender_credentials_data = {
        "procuringEntity": "procuringEntity",
        "tender_token": "tender_token",
        "owner": "owner"
    }
    test_contract = deepcopy(contract)
    test_contract["owner"] = tender_credentials_data["owner"]
    test_contract["tender_token"] = tender_credentials_data["tender_token"]
    session_mock = AsyncMock()
    number_exceptions = 10
    e = [Exception("Test error") for _ in range(number_exceptions)]
    session_mock.get = AsyncMock(
        side_effect=[
            *e,
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_credentials_data}))),
        ]
    )
    with patch("prozorro_bridge_contracting.bridge.asyncio.sleep", AsyncMock()) as mocked_sleep:
        await prepare_contract_data(contract, session_mock)

    assert mocked_logger.warning.call_count == number_exceptions
    assert mocked_sleep.await_count == number_exceptions
    assert mocked_sleep.call_args_list == [call(5) for _ in range(number_exceptions)]
    assert session_mock.get.await_count == number_exceptions + 1
    assert mocked_logger.info.call_count == number_exceptions + 2
    assert test_contract == contract


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_get_tender_contracts_resource_gone(mocked_logger):
    tender_id = "1" * 32
    contract_id = "2" * 32
    tender = {
        "id": tender_id,
        "dateModified": datetime.now().isoformat(),
        "contracts": [{"id": contract_id, "status": "active"}],
    }
    session_mock = AsyncMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=410, text=AsyncMock(return_value=json.dumps({"status": "error"}))),
        ]
    )

    contracts = await get_tender_contracts(tender, session_mock)

    logger_msg = f"Sync contract {contract_id} of tender {tender_id} has been archived"
    extra = journal_context(
        {"MESSAGE_ID": DATABRIDGE_CONTRACT_TO_SYNC},
        {"CONTRACT_ID": contract_id, "TENDER_ID": tender_id},
    )
    mocked_logger.info.assert_called_once_with(logger_msg, extra=extra)
    assert contracts == []


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER", MagicMock())
async def test_get_tender_contracts_not_exists():
    tender_id = "1" * 32
    contract = {"id": "2" * 32, "status": "active"}
    tender = {
        "id": tender_id,
        "dateModified": datetime.now().isoformat(),
        "contracts": [contract],
    }
    mocked_db = AsyncMock()
    mocked_db.has.return_value = False
    session_mock = AsyncMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=404, text=AsyncMock(return_value=json.dumps({"error": "Not found"}))),
        ]
    )

    contracts = await get_tender_contracts(tender, session_mock)

    session_mock.get.assert_called_once()
    assert contracts == [contract]


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_get_tender(mocked_logger):
    tender_data = {
        "id": 2,
        "status": "active",
        "procuringEntity": "procuringEntity",
        "owner": "owner",
        "tender_token": "tender_token",
        "contracts": [{"status": "active", "id": 1}],
    }
    session_mock = AsyncMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=410, text=AsyncMock(return_value=json.dumps({"status": "error"}))),
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_data}))),
        ]
    )
    with patch("prozorro_bridge_contracting.bridge.asyncio.sleep", AsyncMock()) as mocked_sleep:
        tender = await get_tender(tender_data["id"], session_mock)

    assert mocked_logger.warning.call_count == 1
    assert mocked_sleep.await_count == 1
    assert session_mock.get.await_count == 2
    assert tender_data == tender


@patch("prozorro_bridge_contracting.utils.LOGGER", MagicMock())
def test_check_tender():
    tender = {"id": "1", "procurementMethodType": "belowThreshold", "status": "complete"}
    result = check_tender(tender)
    assert result is True

    tender["status"] = "active.qualification"
    result = check_tender(tender)
    assert result is False

    tender["lots"] = [{"status": "complete"}, {"status": "complete"}]
    result = check_tender(tender)
    assert result is True

    tender["lots"][0]["status"] = "unsuccessful"
    result = check_tender(tender)
    assert result is True

    tender["lots"][1]["status"] = "unsuccessful"
    result = check_tender(tender)
    assert result is False

    del tender["lots"]
    tender["procurementMethodType"] = "esco"
    result = check_tender(tender)
    assert result is False


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.utils.LOGGER", MagicMock())
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_process_listing(mocked_logger):
    tender = {
        "id": "1",
        "status": "complete",
        "procurementMethodType": "belowThreshold",
        "dateModified": datetime.now().isoformat()
    }
    session_mock = AsyncMock()
    session_mock.cookie_jar = MagicMock()
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender}))),
        ]
    )

    await process_listing(session_mock, tender)

    extra = journal_context(
        {"MESSAGE_ID": DATABRIDGE_EXCEPTION},
        {"TENDER_ID": tender["id"]}
    )
    mocked_logger.info.assert_called_once_with(f"No contracts found in tender {tender['id']}", extra=extra)
    assert session_mock.post.await_count == 0

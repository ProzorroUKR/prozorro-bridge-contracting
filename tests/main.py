from copy import deepcopy
from datetime import datetime
import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock, call

from prozorro_bridge_contracting.journal_msg_ids import *
from prozorro_bridge_contracting.utils import journal_context
from prozorro_bridge_contracting.bridge import (
    get_tender_credentials,
    get_tender_contracts,
    post_contract,
    prepare_contract_data,
    process_listing,
    HEADERS,
    BASE_URL, check_tender,
)
from prozorro_bridge_contracting.single import get_tender, sync_single_tender


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
@patch("prozorro_bridge_contracting.single.LOGGER")
@patch("prozorro_bridge_contracting.single.get_feed_position", AsyncMock(return_value={}))
async def test_sync_single_tender_contract_skip(mocked_single_logger, mocked_logger):
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
    assert mocked_single_logger.info.call_args_list == [
        call(f"Getting tender {tender_data['id']}"),
        call(f"Got tender {tender_data['id']} in status active"),
        call(f"Getting tender {tender_data['id']} credentials"),
        call(f"Got tender {tender_data['id']} credentials"),
        call(f"Skip contract {contract_data['id']} in status no_active"),
        call(f"Tender {tender_data['id']} does not contain contracts to transfer"),
    ]
    assert mocked_logger.info.call_args_list == [
        call(
            f"Getting credentials for tender {tender_data['id']}",
            extra={"MESSAGE_ID": DATABRIDGE_GET_CREDENTIALS, "JOURNAL_TENDER_ID": tender_data['id']}
        ),
        call(
            f"Got tender {tender_data['id']} credentials",
            extra={"MESSAGE_ID": DATABRIDGE_GOT_CREDENTIALS, "JOURNAL_TENDER_ID": tender_data['id']}
        ),
    ]
    assert session_mock.post.await_count == 0


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
@patch("prozorro_bridge_contracting.single.LOGGER")
@patch("prozorro_bridge_contracting.single.get_feed_position", AsyncMock(return_value={}))
async def test_sync_single_tender_contract_skip_exists(mocked_single_logger, mocked_logger):
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
    assert mocked_single_logger.info.call_args_list == [
        call(f"Getting tender {tender_data['id']}"),
        call(f"Got tender {tender_data['id']} in status active"),
        call(f"Getting tender {tender_data['id']} credentials"),
        call(f"Got tender {tender_data['id']} credentials"),
        call(f"Checking if contract {contract_data['id']} already exists"),
        call(f"Contract exists {contract_data['id']}"),
        call(f"Tender {tender_data['id']} does not contain contracts to transfer"),
    ]
    assert mocked_logger.info.call_args_list == [
        call(
            f"Getting credentials for tender {tender_data['id']}",
            extra={"MESSAGE_ID": DATABRIDGE_GET_CREDENTIALS, "JOURNAL_TENDER_ID": tender_data['id']}
        ),
        call(
            f"Got tender {tender_data['id']} credentials",
            extra={"MESSAGE_ID": DATABRIDGE_GOT_CREDENTIALS, "JOURNAL_TENDER_ID": tender_data['id']}
        ),
    ]
    assert session_mock.post.await_count == 0


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
@patch("prozorro_bridge_contracting.single.LOGGER")
@patch("prozorro_bridge_contracting.single.get_feed_position", AsyncMock(return_value={}))
async def test_sync_single_tender(mocked_single_logger, mocked_logger):
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
    assert mocked_single_logger.info.call_args_list == [
        call(f"Getting tender {tender_data['id']}"),
        call(f"Got tender {tender_data['id']} in status active"),
        call(f"Getting tender {tender_data['id']} credentials"),
        call(f"Got tender {tender_data['id']} credentials"),
        call(f"Checking if contract {contract_data['id']} already exists"),
        call(f"Contract {contract_data['id']} does not exists. Prepare contract for creation."),
        call(f"Extending contract {contract_data['id']} with extra data"),
        call(f"Creating contract {contract_data['id']}"),
        call(f"Contract {contract_data['id']} created"),
        call(f"Successfully transferred contracts: [{contract_data['id']}]")
    ]
    assert mocked_logger.info.call_args_list == [
        call(
            f"Getting credentials for tender {tender_data['id']}",
            extra={"MESSAGE_ID": DATABRIDGE_GET_CREDENTIALS, "JOURNAL_TENDER_ID": tender_data['id']}
        ),
        call(
            f"Got tender {tender_data['id']} credentials",
            extra={"MESSAGE_ID": DATABRIDGE_GOT_CREDENTIALS, "JOURNAL_TENDER_ID": tender_data['id']}
        ),
    ]
    assert session_mock.post.await_count == 1
    assert session_mock.post.await_args[1]["json"]["data"]["id"] == contract_data["id"]
    assert session_mock.post.await_args[1]["json"]["data"]["status"] == contract_data["status"]
    assert session_mock.post.await_args[1]["json"]["data"]["value"] == contract_data["value"]


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.single.LOGGER")
@patch("prozorro_bridge_contracting.single.get_feed_position", AsyncMock(return_value={}))
async def test_sync_single_tender_exception(mocked_single_logger):
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
    assert call(
        f"Successfully transferred contracts: [{contract_data['id']}]"
    ) in mocked_single_logger.info.call_args_list

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
    session_mock.head = AsyncMock(
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
@patch("prozorro_bridge_contracting.single.LOGGER", MagicMock())
async def test_get_tender_contracts_not_exists():
    tender_id = "1" * 32
    contract = {"id": "2" * 32, "status": "active"}
    tender = {
        "id": tender_id,
        "dateModified": datetime.now().isoformat(),
        "contracts": [contract],
    }
    session_mock = AsyncMock()
    session_mock.head = AsyncMock(
        side_effect=[
            MagicMock(status=404, text=AsyncMock(return_value=json.dumps({"error": "Not found"}))),
        ]
    )

    contracts = await get_tender_contracts(tender, session_mock)

    session_mock.head.assert_called_once()
    assert contracts == [contract]


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.single.LOGGER")
async def test_get_tender(mocked_single_logger):
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

    assert mocked_single_logger.warning.call_count == 1
    assert mocked_sleep.await_count == 1
    assert session_mock.get.await_count == 2
    assert tender_data == tender


@patch("prozorro_bridge_contracting.bridge.LOGGER", MagicMock())
def test_check_tender():
    tender = {
        "id": "1",
        "procurementMethodType": "belowThreshold",
        "contracts": [
            {
                "status": "active"
            },
            {
                "status": "active"
            },
        ],
    }
    result = check_tender(tender)
    assert result is True

    tender = {
        "id": "1",
        "procurementMethodType": "belowThreshold",
        "contracts": [
            {
                "status": "active"
            },
            {
                "status": "pending"
            },
        ],
    }
    result = check_tender(tender)
    assert result is True

    tender = {
        "id": "1",
        "procurementMethodType": "belowThreshold",
        "contracts": [
            {
                "status": "pending"
            },
            {
                "status": "pending"
            },
        ],
    }
    result = check_tender(tender)
    assert result is False

    tender = {
        "id": "1",
        "procurementMethodType": "competitiveDialogueUA",
        "contracts": [
            {
                "status": "active"
            },
            {
                "status": "pending"
            },
        ],
    }
    result = check_tender(tender)
    assert result is False

    tender = {
        "id": "1",
        "procurementMethodType": "competitiveDialogueEU",
        "contracts": [
            {
                "status": "active"
            },
            {
                "status": "pending"
            },
        ],
    }
    result = check_tender(tender)
    assert result is False

    tender = {
        "id": "1",
        "procurementMethodType": "esco",
        "contracts": [
            {
                "status": "active"
            },
            {
                "status": "pending"
            },
        ],
    }
    result = check_tender(tender)
    assert result is False


@pytest.mark.asyncio
@patch("prozorro_bridge_contracting.bridge.LOGGER")
async def test_process_listing(mocked_logger):
    tender = {
        "id": "1",
        "procurementMethodType": "belowThreshold",
        "procuringEntity": "procuringEntity",
        "contracts": [
            {
                "id": "1",
                "status": "active"
            },
            {
                "id": "2",
                "status": "pending"
            },
        ],
    }
    tender_credentials_data = {
        "tender_token": "tender_token",
        "owner": "owner",
    }
    session_mock = AsyncMock()
    session_mock.cookie_jar = MagicMock()
    session_mock.head = AsyncMock(
        side_effect=[
            MagicMock(status=404),
        ]
    )
    session_mock.get = AsyncMock(
        side_effect=[
            MagicMock(status=200, text=AsyncMock(return_value=json.dumps({"data": tender_credentials_data}))),
        ]
    )
    session_mock.post = AsyncMock(
        return_value=MagicMock(
            status=201,
            json=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
            text=AsyncMock(return_value=json.dumps({"data": ["test1", "test2"]})),
        )
    )

    await process_listing(session_mock, tender)

    assert mocked_logger.info.mock_calls == [
        call(
            f"Found tender {tender['id']} with active contracts",
            extra={
                "MESSAGE_ID": DATABRIDGE_FOUND_ACTIVE_CONTRACTS,
                "JOURNAL_TENDER_ID": tender['id'],
            },
        ),
        call(
            f"Sync contract {tender['contracts'][0]['id']} of tender {tender['id']}",
            extra={
                "MESSAGE_ID": DATABRIDGE_CONTRACT_TO_SYNC,
                "JOURNAL_CONTRACT_ID": tender['contracts'][0]['id'],
                "JOURNAL_TENDER_ID": tender['id'],
            },
        ),
        call(
            f"Getting credentials for tender {tender['id']}",
            extra={
                "MESSAGE_ID": DATABRIDGE_GET_CREDENTIALS,
                "JOURNAL_TENDER_ID": tender['id'],
            },
        ),
        call(
            f"Got tender {tender['id']} credentials",
            extra={
                "MESSAGE_ID": DATABRIDGE_GOT_CREDENTIALS,
                "JOURNAL_TENDER_ID": tender['id'],
            },
        ),
        call(
            f"Creating contract {tender['contracts'][0]['id']} of tender 1",
            extra={
                "MESSAGE_ID": DATABRIDGE_CREATE_CONTRACT,
                "JOURNAL_CONTRACT_ID": tender['contracts'][0]['id'],
                "JOURNAL_TENDER_ID": tender['id'],
            },
        ),
        call(
            f"Successfully created contract {tender['contracts'][0]['id']} of tender {tender['id']}",
            extra={
                "MESSAGE_ID": DATABRIDGE_CONTRACT_CREATED,
                "JOURNAL_CONTRACT_ID": tender['contracts'][0]['id'],
                "JOURNAL_TENDER_ID": tender['id'],
            },
        ),
    ]
    assert session_mock.post.mock_calls == [
        call(
            f"{BASE_URL}/contracts",
            json={
                'data': {
                    "id": "1",
                    "status": "active",
                    "procuringEntity": "procuringEntity",
                    "owner": "owner", 
                    "tender_token": "tender_token",
                    "tender_id": "1",
                }
            },
            headers=HEADERS
        ),
    ]

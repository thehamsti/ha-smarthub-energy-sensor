"""Test the SmartHub Coop Energy config flow."""
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType

from custom_components.smarthub.const import DOMAIN
from custom_components.smarthub.exceptions import SmartHubAuthenticationError, SmartHubConnectionError

async def test_form(hass: HomeAssistant) -> None:
    """Test we get the form."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] == FlowResultType.FORM
    assert result["errors"] == {}

    with patch(
        "custom_components.smarthub.config_flow.SmartHubAPI.get_token",
        return_value="test-token",
    ), patch(
        "custom_components.smarthub.config_flow.SmartHubAPI.close",
        return_value=None,
    ), patch(
        "custom_components.smarthub.async_setup_entry",
        return_value=True,
    ) as mock_setup_entry:
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                "email": "test@example.com",
                "password": "test-password",
                "account_id": "12345",
                "host": "test.smarthub.coop",
                "timezone": "UTC",
                "poll_interval": 360,
            },
        )
        await hass.async_block_till_done()

    assert result2["type"] == FlowResultType.CREATE_ENTRY
    assert result2["title"] == "SmartHub"
    assert result2["data"] == {
        "email": "test@example.com",
        "password": "test-password",
        "account_id": "12345",
        "host": "test.smarthub.coop",
        "timezone": "UTC",
        "poll_interval": 360,
    }
    assert len(mock_setup_entry.mock_calls) == 1


async def test_form_invalid_auth(hass: HomeAssistant) -> None:
    """Test we handle invalid auth."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )

    with patch(
        "custom_components.smarthub.config_flow.SmartHubAPI.get_token",
        side_effect=SmartHubAuthenticationError,
    ), patch(
        "custom_components.smarthub.config_flow.SmartHubAPI.close",
        return_value=None,
    ):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                "email": "test@example.com",
                "password": "test-password",
                "account_id": "12345",
                "host": "test.smarthub.coop",
                "timezone": "UTC",
                "poll_interval": 360,
            },
        )

    assert result2["type"] == FlowResultType.FORM
    assert result2["errors"] == {"base": "invalid_auth"}


async def test_form_cannot_connect(hass: HomeAssistant) -> None:
    """Test we handle cannot connect error."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )

    with patch(
        "custom_components.smarthub.config_flow.SmartHubAPI.get_token",
        side_effect=SmartHubConnectionError,
    ), patch(
        "custom_components.smarthub.config_flow.SmartHubAPI.close",
        return_value=None,
    ):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                "email": "test@example.com",
                "password": "test-password",
                "account_id": "12345",
                "host": "test.smarthub.coop",
                "timezone": "UTC",
                "poll_interval": 360,
            },
        )

    assert result2["type"] == FlowResultType.FORM
    assert result2["errors"] == {"base": "cannot_connect"}


async def test_form_unknown_exception(hass: HomeAssistant) -> None:
    """Test we handle unknown exception."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )

    with patch(
        "custom_components.smarthub.config_flow.SmartHubAPI.get_token",
        side_effect=Exception,
    ), patch(
        "custom_components.smarthub.config_flow.SmartHubAPI.close",
        return_value=None,
    ):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                "email": "test@example.com",
                "password": "test-password",
                "account_id": "12345",
                "host": "test.smarthub.coop",
                "timezone": "UTC",
                "poll_interval": 360,
            },
        )

    assert result2["type"] == FlowResultType.FORM
    assert result2["errors"] == {"base": "unknown"}


async def test_user_flow_without_poll_interval_uses_adaptive_polling(hass):
    """No poll_interval in the entry: the coordinator schedules polls adaptively."""
    from unittest.mock import AsyncMock, patch
    from homeassistant import config_entries
    from custom_components.smarthub.const import DOMAIN

    await hass.config.async_set_time_zone("America/Chicago")
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
    schema_keys = {str(k): k for k in result["data_schema"].schema}
    assert schema_keys["timezone"].default() == "America/Chicago"
    with patch("custom_components.smarthub.config_flow.SmartHubAPI") as api_cls, \
            patch("custom_components.smarthub.async_setup_entry", return_value=True):
        api_cls.return_value.get_token = AsyncMock(return_value="token")
        api_cls.return_value.close = AsyncMock()
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {
            "email": "test@example.com", "password": "testpass", "account_id": "123456",
            "host": "test.smarthub.coop", "timezone": "America/Chicago",
        })
    assert result["type"] == "create_entry"
    assert "poll_interval" not in result["data"]

"""Diagnostics support for SmartHub."""
from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

TO_REDACT = {"email", "password", "mfa_totp", "userId", "primary_username", "account_id", "accountNumber"}


async def async_get_config_entry_diagnostics(hass: HomeAssistant, entry: ConfigEntry) -> dict[str, Any]:
    """Return diagnostics for a config entry (credentials and account identifiers redacted)."""
    coordinator = getattr(entry, "runtime_data", None)
    locations: dict[str, Any] = {}
    if coordinator is not None and coordinator.data:
        for location_id, data in coordinator.data.items():
            location = data.get("location")
            locations[location_id] = {
                "description": getattr(location, "description", None),
                "provider": getattr(location, "provider", None),
                **{k: (v.isoformat() if hasattr(v, "isoformat") else v)
                   for k, v in data.items() if k != "location"},
            }
    return {
        "entry": {
            "data": async_redact_data(dict(entry.data), TO_REDACT),
            "options": async_redact_data(dict(entry.options), TO_REDACT),
        },
        "locations": locations,
        "coordinator": coordinator.diagnostics() if coordinator is not None else None,
        "last_update_success": getattr(coordinator, "last_update_success", None),
    }

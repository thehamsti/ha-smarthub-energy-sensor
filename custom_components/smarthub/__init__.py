"""
Custom integration to integrate SmartHub Coop energy sensors with Home Assistant.

For more details about this integration, please refer to
https://github.com/gagata/ha-smarthub-energy-sensor
"""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryError
from homeassistant.util import dt as dt_util

from .api import SmartHubAPI
from .const import CONF_TIMEZONE, DOMAIN
from .coordinator import SmartHubDataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up SmartHub from a config entry."""
    config = entry.data

    required_fields = ["email", "password", "account_id", "host"]
    missing_fields = [field for field in required_fields if not config.get(field)]
    if missing_fields:
        _LOGGER.error("Missing required configuration fields: %s", missing_fields)
        raise ConfigEntryError(f"Missing configuration fields: {missing_fields}")

    # SmartHub reports interval timestamps in the utility's local wall-clock time.
    # Entries created before the timezone option existed have no value; the
    # utility serves this Home Assistant's location, so use HA's time zone rather
    # than GMT (which shifted every statistic by the UTC offset).
    tz_name = config.get(CONF_TIMEZONE) or hass.config.time_zone
    tz = await dt_util.async_get_time_zone(tz_name)
    if tz is None:
        raise ConfigEntryError(f"Unknown time zone {tz_name!r}")

    api = SmartHubAPI(
        email=config["email"],
        password=config["password"],
        account_id=config["account_id"],
        timezone=tz_name,
        mfa_totp=config.get("mfa_totp", ""),
        host=config["host"],
    )

    coordinator = SmartHubDataUpdateCoordinator(hass, api, entry, tz)
    try:
        await coordinator.async_load_store()
        # Raises ConfigEntryNotReady on transient failures (HA retries setup) and
        # ConfigEntryError when the utility rejects the credentials.
        await coordinator.async_config_entry_first_refresh()
    except BaseException:
        await api.close()
        raise
    entry.runtime_data = coordinator

    try:
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    except BaseException:
        await coordinator.async_shutdown_api()
        raise
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinator = getattr(entry, "runtime_data", None)
        if isinstance(coordinator, SmartHubDataUpdateCoordinator):
            await coordinator.async_shutdown_api()
        hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    return unload_ok

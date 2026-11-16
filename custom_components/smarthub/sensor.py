"""SmartHub energy sensor platform."""
from __future__ import annotations

from datetime import datetime
import logging
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import slugify

from .api import SmartHubLocation
from .const import (
    ATTR_ACCOUNT_ID,
    ATTR_LAST_READING_TIME,
    ATTR_LOCATION_ID,
    DOMAIN,
    METER_NAME,
)
from .coordinator import SmartHubDataUpdateCoordinator, statistic_id

_LOGGER = logging.getLogger(__name__)

# Kept for backwards compatibility with code importing it from here.
__all__ = ["SmartHubDataUpdateCoordinator"]


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the SmartHub sensor platform."""
    coordinator: SmartHubDataUpdateCoordinator = config_entry.runtime_data
    config = config_entry.data
    entities: list[SensorEntity] = []
    for key, entry in (coordinator.data or {}).items():
        location: SmartHubLocation = entry["location"]
        if not entry.get("primary", True):
            entities.extend(_meter_entities(coordinator, config_entry, config, location, key, entry))
            continue
        entities.append(SmartHubEnergySensor(coordinator, config_entry, config, location))
        entities.append(SmartHubMonthToDateSensor(coordinator, config_entry, config, location, "import_mtd"))
        if entry.get("export_mtd") is not None:
            entities.append(SmartHubMonthToDateSensor(coordinator, config_entry, config, location, "export_mtd"))
        entities.append(SmartHubLastReadingSensor(coordinator, config_entry, config, location))
    async_add_entities(entities)
    _LOGGER.debug("%d SmartHub sensor entities added", len(entities))


def _meter_entities(coordinator, config_entry, config, location, key, entry) -> list[SensorEntity]:
    """Entities for an additional (non-grid) meter at the location."""
    out: list[SensorEntity] = [SmartHubMeterMonthSensor(coordinator, config_entry, config, location, key, entry["meter"],
                                                        "import_mtd")]
    if entry.get("export_mtd") is not None:
        out.append(SmartHubMeterMonthSensor(coordinator, config_entry, config, location, key, entry["meter"],
                                            "export_mtd"))
    out.append(SmartHubLastReadingSensor(coordinator, config_entry, config, location, key=key, meter=entry["meter"]))
    return out


class _SmartHubBase(CoordinatorEntity[SmartHubDataUpdateCoordinator], SensorEntity):
    """Common plumbing: device, availability (last good value) and attributes."""

    def __init__(
        self,
        coordinator: SmartHubDataUpdateCoordinator,
        config_entry: ConfigEntry,
        config: dict[str, Any],
        location: SmartHubLocation,
    ) -> None:
        super().__init__(coordinator)
        self._config_entry = config_entry
        self._config = config
        self.location = location
        self._account_id = str(config.get("account_id", "Unknown"))
        self._data_key = location.id

    @property
    def _entry(self) -> dict[str, Any]:
        return (self.coordinator.data or {}).get(self._data_key) or {}

    @property
    def available(self) -> bool:
        # Keep the last good value through transient failures; the coordinator only
        # drops data if the integration is unloaded.
        return self.native_value is not None

    @property
    def device_info(self) -> dict[str, Any]:
        host = self._config.get("host", "Unknown")
        return {
            "identifiers": {(DOMAIN, self._config_entry.unique_id or self._config_entry.entry_id)},
            "name": f"{self.location.provider} SmartHub Energy Monthly Usage ({self._account_id} - {self.location.description})",
            "manufacturer": "SmartHub Coop",
            "model": "Energy Monitor",
            "configuration_url": f"https://{host}",
        }

    def _common_attributes(self) -> dict[str, Any]:
        entry = self._entry
        attrs: dict[str, Any] = {
            ATTR_ACCOUNT_ID: self._config.get("account_id"),
            ATTR_LOCATION_ID: self.location.id,
        }
        if entry.get("data_end"):
            attrs[ATTR_LAST_READING_TIME] = entry["data_end"].astimezone(self.coordinator.tz)
        if entry.get("meter"):
            attrs[METER_NAME] = entry["meter"]
        attrs["data_stale"] = bool(entry.get("stale"))
        return attrs


class SmartHubEnergySensor(_SmartHubBase):
    """Month-to-date NET grid energy (import - export). Existing entity; unique_id unchanged."""

    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_state_class = SensorStateClass.TOTAL
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    _attr_suggested_display_precision = 2
    _attr_icon = "mdi:lightning-bolt"

    def __init__(self, coordinator, config_entry, config, location) -> None:
        super().__init__(coordinator, config_entry, config, location)
        self._attr_unique_id = f"{config_entry.unique_id}_{location.id}_energy"
        self._attr_name = (
            f"{location.provider} SmartHub Energy Monthly Usage - {self._account_id} {location.description}"
        )

    @property
    def native_value(self) -> float | None:
        return self._entry.get("net_mtd")

    @property
    def last_reset(self) -> datetime | None:
        return self._entry.get("month_start")

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        attrs = self._common_attributes()
        entry = self._entry
        attrs["import_mtd"] = entry.get("import_mtd")
        if entry.get("export_mtd") is not None:
            attrs["export_mtd"] = entry.get("export_mtd")
        if entry.get("meter"):
            attrs["statistic_ids"] = [
                statistic_id(role, self._account_id, self.location.id, entry["meter"])
                for role in ("import", "export", "net")
            ]
        return attrs


class SmartHubMonthToDateSensor(_SmartHubBase):
    """Month-to-date grid import or export."""

    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_state_class = SensorStateClass.TOTAL
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    _attr_suggested_display_precision = 2

    _KINDS = {
        "import_mtd": ("Grid Import This Month", "grid_import_this_month", "mdi:transmission-tower-import"),
        "export_mtd": ("Grid Export This Month", "grid_export_this_month", "mdi:transmission-tower-export"),
    }

    def __init__(self, coordinator, config_entry, config, location, kind: str) -> None:
        super().__init__(coordinator, config_entry, config, location)
        self._kind = kind
        label, slug, icon = self._KINDS[kind]
        meter = (coordinator.data or {}).get(location.id, {}).get("meter", "meter")
        self._attr_unique_id = f"{config_entry.entry_id}_{location.id}_{meter}_{kind}"
        self._attr_name = f"SmartHub {label} - {self._account_id} {location.description}"
        self._attr_icon = icon
        self.entity_id = f"sensor.smarthub_{slug}_{slugify(self._account_id)}_{slugify(location.description or location.id)}"

    @property
    def native_value(self) -> float | None:
        return self._entry.get(self._kind)

    @property
    def last_reset(self) -> datetime | None:
        return self._entry.get("month_start")

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return self._common_attributes()


class SmartHubLastReadingSensor(_SmartHubBase):
    """End time of the newest interval the utility has published (freshness)."""

    _attr_device_class = SensorDeviceClass.TIMESTAMP
    _attr_icon = "mdi:clock-check-outline"

    def __init__(self, coordinator, config_entry, config, location, key: str | None = None,
                 meter: str | None = None) -> None:
        super().__init__(coordinator, config_entry, config, location)
        if key is not None:
            self._data_key = key
        meter = meter or (coordinator.data or {}).get(location.id, {}).get("meter", "meter")
        self._attr_unique_id = f"{config_entry.entry_id}_{location.id}_{meter}_last_reading"
        if key is None:
            self._attr_name = f"SmartHub Last Meter Reading - {self._account_id} {location.description}"
            self.entity_id = (f"sensor.smarthub_last_reading_{slugify(self._account_id)}_"
                              f"{slugify(location.description or location.id)}")
        else:
            self._attr_name = f"SmartHub Meter {meter} Last Reading - {self._account_id} {location.description}"
            self.entity_id = f"sensor.smarthub_meter_{slugify(meter)}_last_reading"

    @property
    def native_value(self) -> datetime | None:
        return self._entry.get("data_end")

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        attrs = self._common_attributes()
        attrs.pop(ATTR_LAST_READING_TIME, None)
        return attrs


class SmartHubMeterMonthSensor(_SmartHubBase):
    """Month-to-date energy of an additional meter, by register direction."""

    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_state_class = SensorStateClass.TOTAL
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    _attr_suggested_display_precision = 2
    _attr_icon = "mdi:meter-electric"
    _KINDS = {"import_mtd": ("Energy This Month", "energy_this_month", "forward_mtd"),
              "export_mtd": ("Reverse Energy This Month", "reverse_energy_this_month", "reverse_mtd")}

    def __init__(self, coordinator, config_entry, config, location, key: str, meter: str, kind: str) -> None:
        super().__init__(coordinator, config_entry, config, location)
        self._data_key = key
        self._kind = kind
        label, slug, uid = self._KINDS[kind]
        self._attr_unique_id = f"{config_entry.entry_id}_{location.id}_{meter}_{uid}"
        self._attr_name = f"SmartHub Meter {meter} {label} - {self._account_id} {location.description}"
        self.entity_id = f"sensor.smarthub_meter_{slugify(meter)}_{slug}"

    @property
    def native_value(self) -> float | None:
        return self._entry.get(self._kind)

    @property
    def last_reset(self) -> datetime | None:
        return self._entry.get("month_start")

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        attrs = self._common_attributes()
        if self._entry.get("meter"):
            attrs["statistic_id"] = self.coordinator._sid(
                "import" if self._kind == "import_mtd" else "export", self.location.id, self._entry["meter"])
        return attrs

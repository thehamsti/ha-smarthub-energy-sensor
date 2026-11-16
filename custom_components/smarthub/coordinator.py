"""Update coordinator for SmartHub: adaptive polling, statistics and keep-last-good.

Per refresh and service location:
1. *Probe* (cheap): fetch HOURLY data starting 2 h before the newest known interval.
   If nothing newer is published (and a reconciliation is not due), keep the current
   snapshot unchanged (no state writes).
2. *Full update*: fetch the rewrite window (current local month + 2-day grace, and at
   least 7 days), recompute month-to-date totals, and rewrite the hourly external
   statistics contiguously from the exact predecessor row. Rewrites that would lower
   a recorded hour using incomplete data are refused.
3. On any error, keep the previous snapshot (marked stale) and back off. After a
   restart, a persisted snapshot is used if the utility is unreachable.

Statistics use new IDs (smarthub:grid_import|grid_export|grid_net_<acct>_<loc>_<meter>)
aligned to real local time. A one-time background backfill populates them from the
start of the legacy statistics; regular writes begin only after it is verified.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone, tzinfo
from functools import partial
import logging
from typing import Any

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, UnitOfEnergy
from homeassistant.core import CoreState, HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util
from homeassistant.util.unit_conversion import EnergyConverter

try:
    from homeassistant.components.recorder.models import StatisticMeanType
except ImportError:  # pragma: no cover - older HA
    from enum import Enum

    class StatisticMeanType(str, Enum):  # type: ignore[no-redef]
        NONE = "none"

from . import processing as P
from .api import Aggregation, SmartHubAPI, SmartHubLocation
from .const import (
    BACKFILL_CHUNK_DAYS,
    BACKFILL_CHUNK_PAUSE,
    BACKFILL_MAX_DAYS,
    BACKFILL_RETRIES,
    BACKFILL_RETRY_DELAY,
    CONF_EXCLUDED_METERS,
    CONF_METER_NUMBER,
    CONF_POLL_INTERVAL,
    DOMAIN,
    ELECTRIC_SERVICE,
    FETCH_DEADLINE,
    VERIFY_TIMEOUT,
    GAP_CONFIRM,
    GAP_REFETCH_PAUSE,
    HISTORICAL_IMPORT_DAYS,
    LOCATIONS_TTL,
    MAX_ARRIVALS,
    MAX_POLL_INTERVAL,
    MIN_POLL_INTERVAL,
    MONTH_GRACE,
    PROBE_OVERLAP,
    RECONCILE_EVERY,
    REWRITE_LOOKBACK,
    STORAGE_VERSION,
)
from .exceptions import SmartHubAuthenticationError, SmartHubError

_LOGGER = logging.getLogger(__name__)

ROLES = (P.ROLE_IMPORT, P.ROLE_EXPORT, P.ROLE_NET)
_ROLE_LABEL = {P.ROLE_IMPORT: "Grid Import", P.ROLE_EXPORT: "Grid Export", P.ROLE_NET: "Grid Net"}
_FAR_FUTURE = datetime(2100, 1, 1, tzinfo=timezone.utc)
SCHEMA = 1  # statistics derivation schema (registers, local-time alignment)


_METER_ROLE = {P.ROLE_IMPORT: "forward", P.ROLE_EXPORT: "reverse", P.ROLE_NET: "net"}


def statistic_id(role: str, account_id: str, location_id: str, meter: str, primary: bool = True) -> str:
    """External statistic id for a role.

    The primary (grid connection) meter uses grid_import/grid_export/grid_net. Any
    additional meter at the location is named by its register direction, because its
    meaning (e.g. a production meter) can't be inferred from the utility metadata.
    """
    if primary:
        return f"{DOMAIN}:grid_{role}_{account_id}_{location_id}_{meter}".lower()
    return f"{DOMAIN}:meter_{_METER_ROLE[role]}_{account_id}_{location_id}_{meter}".lower()


def data_key(location_id: str, meter: str, primary: bool) -> str:
    """coordinator.data / Store key: the location id for the primary meter, else location:meter."""
    return location_id if primary else f"{location_id}:{meter}"


def legacy_statistic_id(account_id: str, location_id: str) -> str:
    return f"{DOMAIN}:smarthub_energy_sensor_{account_id}_{location_id}"


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


class SmartHubDataUpdateCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Fetch SmartHub data, keep statistics current and schedule polls adaptively."""

    def __init__(
        self,
        hass: HomeAssistant,
        api: SmartHubAPI,
        config_entry: ConfigEntry,
        tz: tzinfo,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=f"{DOMAIN}_{config_entry.entry_id}",
            update_interval=P.LEARNING_INTERVAL,
            always_update=False,
        )
        self.api = api
        self.tz = tz
        self.account_id = str(config_entry.data.get("account_id", "unknown"))
        self.configured_meter = (
            config_entry.options.get(CONF_METER_NUMBER) or config_entry.data.get(CONF_METER_NUMBER) or None
        )
        poll = config_entry.options.get(CONF_POLL_INTERVAL) or config_entry.data.get(CONF_POLL_INTERVAL)
        self.fixed_interval: timedelta | None = None
        if poll:
            minutes = min(max(int(poll), MIN_POLL_INTERVAL), MAX_POLL_INTERVAL)
            self.fixed_interval = timedelta(minutes=minutes)
        self._store: Store[dict[str, Any]] = Store(hass, STORAGE_VERSION, f"{DOMAIN}.{config_entry.entry_id}")
        self._stored: dict[str, Any] = {}
        self._locations: list[SmartHubLocation] | None = None
        self._locations_at: datetime | None = None
        self._refresh_lock = asyncio.Lock()
        self._api_lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()
        self._backfill_tasks: dict[str, asyncio.Task] = {}
        # runtime health (diagnostics; kept out of self.data to avoid state churn)
        self.consecutive_failures = 0
        self.last_error: str | None = None
        self.last_success: datetime | None = None
        self.last_refresh_ok: datetime | None = None
        self.counters = {"probes": 0, "full_updates": 0, "reconciliations": 0, "failures": 0,
                         "stat_rows_written": 0, "refused_writes": 0}
        self.schedule: P.Schedule | None = None
        self.backfill_status: dict[str, str] = {}
        self.batch_info: dict[str, dict[str, Any]] = {}
        self.daily_check: dict[str, Any] = {}
        self.last_written: dict[str, str] = {}
        self.meter_errors: dict[str, str] = {}
        self.excluded_meters: set[str] = {
            str(m) for m in (config_entry.options.get(CONF_EXCLUDED_METERS)
                             or config_entry.data.get(CONF_EXCLUDED_METERS) or [])
        }

    # ------------------------------------------------------------------ store
    async def async_load_store(self) -> None:
        self._stored = await self._store.async_load() or {}
        for key in ("arrivals", "backfill", "data_end", "meter", "layout", "last_full", "snapshot"):
            self._stored.setdefault(key, [] if key == "arrivals" else {})
        self._stored.setdefault("gaps", {"pending": {}, "accepted": {}})

    @callback
    def _save(self) -> None:
        self._store.async_delay_save(lambda: self._stored, 2)

    def _arrivals(self) -> list[tuple[datetime, datetime]]:
        out = []
        for seen, end in self._stored.get("arrivals", []):
            s, e = dt_util.parse_datetime(seen), dt_util.parse_datetime(end)
            if s and e:
                out.append((s, e))
        return out

    def _known_end(self, location_id: str) -> datetime | None:
        value = self._stored["data_end"].get(location_id)
        return dt_util.parse_datetime(value) if value else None

    def _backfill_record(self, key: str) -> dict[str, Any]:
        return self._stored["backfill"].get(key) or {}

    def backfill_done(self, key: str) -> bool:
        rec = self._backfill_record(key)
        return bool(rec.get("done")) and rec.get("schema") == SCHEMA and rec.get("tz") == str(self.tz)

    def _preferred_meter(self, location_id: str) -> str | None:
        return self.configured_meter or self._stored["meter"].get(location_id)

    def _is_primary(self, location_id: str, meter: str) -> bool:
        primary = self._preferred_meter(location_id)
        return primary is None or primary == meter

    def _sid(self, role: str, location_id: str, meter: str) -> str:
        return statistic_id(role, self.account_id, location_id, meter, self._is_primary(location_id, meter))

    # ------------------------------------------------------------- API access
    async def _api_call(self, func, *args):
        """Serialize all utility traffic and bound each call's total time."""
        async with self._api_lock:
            async with asyncio.timeout(FETCH_DEADLINE.total_seconds()):
                return await func(*args)

    async def _fetch_payload(
        self, location: SmartHubLocation, start: datetime, end: datetime, aggregation: Aggregation = Aggregation.HOURLY
    ) -> dict[str, Any]:
        return await self._api_call(self.api.fetch_usage, location, aggregation, start, end)

    async def _fetch_batch(
        self, location: SmartHubLocation, start: datetime, end: datetime, *,
        window_end: datetime = _FAR_FUTURE, truncate: bool = True, meter: str | None = None,
    ) -> P.UsageBatch:
        payload = await self._fetch_payload(location, start, end)
        return await self._batch(payload, location, meter or self._preferred_meter(location.id),
                                 start, window_end, truncate)

    async def _batch(
        self, payload: dict[str, Any], location: SmartHubLocation, meter: str | None,
        start: datetime, window_end: datetime = _FAR_FUTURE, truncate: bool = True,
    ) -> P.UsageBatch:
        batch = await self.hass.async_add_executor_job(
            partial(P.build_batch, payload, self.tz, meter, window=(start, window_end), truncate=truncate)
        )
        key = data_key(location.id, batch.meter, self._is_primary(location.id, batch.meter))
        pinned = self._stored["layout"].get(key)
        layout = {"meter": batch.meter, "mode": batch.mode, "channels": batch.channels}
        if pinned and pinned != layout:
            # Never silently switch derivation (e.g. registers -> NET split) under the same statistic IDs.
            raise P.IncompleteDataError(f"SmartHub channel layout changed: pinned {pinned}, got {layout}")
        return batch

    # ---------------------------------------------------------------- refresh
    async def _async_update_data(self) -> dict[str, Any]:
        async with self._refresh_lock:
            now = dt_util.utcnow()
            try:
                data = await self._refresh(now)
            except Exception as err:  # noqa: BLE001 - every failure keeps the last good value
                data = self._handle_failure(err)
            else:
                if self.consecutive_failures:
                    _LOGGER.info("SmartHub data fetch recovered after %d failure(s)", self.consecutive_failures)
                self.consecutive_failures = 0
                self.last_error = None
                self.last_success = now
                self.last_refresh_ok = now
            finally:
                self._reschedule(now)
            return data

    def _handle_failure(self, err: Exception) -> dict[str, Any]:
        self.consecutive_failures += 1
        self.counters["failures"] += 1
        self.last_error = f"{type(err).__name__}: {err}"
        previous = self.data or (None if isinstance(err, SmartHubAuthenticationError) else self._snapshot_data())
        if not previous:
            if isinstance(err, SmartHubAuthenticationError):
                raise ConfigEntryError(f"SmartHub rejected the credentials: {err}") from err
            raise UpdateFailed(f"SmartHub fetch failed: {self.last_error}") from err
        if not isinstance(err, (SmartHubError, P.IncompleteDataError, TimeoutError)):
            _LOGGER.exception("Unexpected SmartHub error; keeping last good values")
        elif self.consecutive_failures == 1:
            _LOGGER.warning("SmartHub fetch failed (%s); keeping last good values", self.last_error)
        else:
            _LOGGER.debug("SmartHub fetch failed again (#%d): %s", self.consecutive_failures, self.last_error)
        return {loc: entry if entry.get("stale") else {**entry, "stale": True} for loc, entry in previous.items()}

    def _snapshot_data(self) -> dict[str, Any] | None:
        """Last good values persisted before the restart, marked stale."""
        out = {}
        for key, snap in (self._stored.get("snapshot") or {}).items():
            location_id = key.split(":", 1)[0]
            try:
                out[key] = {
                    "location": SmartHubLocation(id=location_id, service=ELECTRIC_SERVICE,
                                                 description=snap["description"], provider=snap["provider"]),
                    "primary": ":" not in key,
                    "meter": snap["meter"],
                    "mode": snap["mode"],
                    "month_start": dt_util.parse_datetime(snap["month_start"]).astimezone(self.tz),
                    "import_mtd": snap["import_mtd"],
                    "export_mtd": snap["export_mtd"],
                    "net_mtd": snap["net_mtd"],
                    "data_end": dt_util.parse_datetime(snap["data_end"]),
                    "stale": True,
                }
            except (KeyError, TypeError, AttributeError, ValueError):
                return None
        if out:
            _LOGGER.warning("SmartHub unreachable at startup; showing last good values from %s",
                            ", ".join(v["data_end"].isoformat() for v in out.values()))
        return out or None

    @callback
    def _reschedule(self, now: datetime) -> None:
        ends = [e for e in (self._known_end(k) for k in self._stored["data_end"] if ":" not in k) if e]
        self.schedule = P.compute_schedule(
            now, max(ends) if ends else None, self._arrivals(), self.consecutive_failures, self.fixed_interval
        )
        not_before = getattr(self.api, "not_before", None)
        if not_before is not None and not_before - now > self.schedule.wait:
            self.schedule = P.Schedule(not_before - now + timedelta(seconds=30), "server_retry_after")
        self.update_interval = self.schedule.wait
        _LOGGER.debug("Next SmartHub poll in %s (%s)", self.schedule.wait, self.schedule.mode)

    async def _get_locations(self, now: datetime) -> list[SmartHubLocation]:
        if self._locations is None or self._locations_at is None or now - self._locations_at > LOCATIONS_TTL:
            locations = await self._api_call(self.api.get_service_locations)
            if not locations:
                raise P.IncompleteDataError("No active electric service locations returned")
            self._locations, self._locations_at = locations, now
        return self._locations

    async def _refresh(self, now: datetime) -> dict[str, Any]:
        locations = await self._get_locations(now)
        previous = self.data or {}
        result: dict[str, Any] = {}
        for location in locations:
            known_end = self._known_end(location.id)
            prev_entry = previous.get(location.id)
            last_full = dt_util.parse_datetime(self._stored["last_full"].get(location.id) or "")
            reconcile_due = last_full is None or now - last_full >= RECONCILE_EVERY
            if prev_entry is not None and known_end is not None:
                probe_end = await self._probe(location, known_end, now)
                if probe_end <= known_end and not reconcile_due:
                    for key, entry in previous.items():
                        if key == location.id or key.startswith(f"{location.id}:"):
                            result[key] = entry if not entry.get("stale") else {**entry, "stale": False}
                    continue
                if probe_end <= known_end:
                    self.counters["reconciliations"] += 1
            result.update(await self._full_update(location, now, known_end, previous))
        return previous if result == previous else result

    async def _probe(self, location: SmartHubLocation, known_end: datetime, now: datetime) -> datetime:
        """Return the newest common data end currently published."""
        self.counters["probes"] += 1
        start = min(known_end, now) - PROBE_OVERLAP
        batch = await self._fetch_batch(location, start, now)
        return batch.data_end

    async def _full_update(
        self, location: SmartHubLocation, now: datetime, known_end: datetime | None,
        previous: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """One fetch for the location; the primary meter must succeed, others are isolated."""
        self.counters["full_updates"] += 1
        previous = previous or {}
        candidates = [P.month_start(now - MONTH_GRACE, self.tz), now - REWRITE_LOOKBACK]
        if known_end is not None:
            candidates.append(P.month_start(known_end - timedelta(minutes=1), self.tz))
        for key, rec in self._stored["backfill"].items():
            if key.startswith(f"{location.id}_") and self.backfill_done(key):
                meter = rec.get("meter") or key[len(location.id) + 1:]
                last_rows = [await self._last_row_start(self._sid(r, location.id, meter)) for r in ROLES]
                present = [r for r in last_rows if r is not None]
                if present:
                    candidates.append(min(present) - REWRITE_LOOKBACK)
        fetch_start = P.floor_hour(min(candidates)).astimezone(self.tz)

        payload = await self._fetch_payload(location, fetch_start, now)
        batch = await self._batch(payload, location, self._preferred_meter(location.id), fetch_start)
        if known_end is not None and batch.data_end < known_end:
            raise P.IncompleteDataError(
                f"Response ends {batch.data_end.isoformat()} before already-recorded {known_end.isoformat()}"
            )
        if location.id not in self._stored["meter"] and not self.configured_meter:
            self._stored["meter"][location.id] = batch.meter  # pin the physical (grid) meter
        results = {location.id: await self._process_meter(location, batch, fetch_start, now, primary=True)}

        # Additional meters at the location (e.g. a separately metered production meter).
        for meter in sorted({row["meter"] for row in batch.inventory} - {batch.meter} - self.excluded_meters):
            key = data_key(location.id, meter, False)
            try:
                extra = await self._batch(payload, location, meter, fetch_start)
                results[key] = await self._process_meter(location, extra, fetch_start, now, primary=False)
                self.meter_errors.pop(key, None)
            except (P.IncompleteDataError, SmartHubError) as err:
                self.meter_errors[key] = f"{type(err).__name__}: {err}"
                _LOGGER.debug("SmartHub meter %s not updated: %s", meter, err)
                if (prev := previous.get(key)) is not None:
                    results[key] = prev if prev.get("stale") else {**prev, "stale": True}

        await self._daily_crosscheck(location, batch, fetch_start, now)

        # Learn publish timing only when the previous successful refresh bounds it.
        if (known_end is not None and batch.data_end > known_end and self.last_refresh_ok is not None
                and now - self.last_refresh_ok <= timedelta(hours=2)):
            arrivals = self._stored["arrivals"]
            arrivals.append([now.isoformat(), batch.data_end.isoformat()])
            del arrivals[:-MAX_ARRIVALS]
        self._stored["last_full"][location.id] = now.isoformat()
        self._save()
        return results

    async def _process_meter(
        self, location: SmartHubLocation, batch: P.UsageBatch, fetch_start: datetime, now: datetime, *,
        primary: bool,
    ) -> dict[str, Any]:
        """Validate, resolve, publish MTD and write statistics for one meter of a fetched window."""
        key = data_key(location.id, batch.meter, primary)
        bkey = f"{location.id}_{batch.meter}"
        rec = self._backfill_record(bkey)
        origin = dt_util.parse_datetime(rec.get("data_start") or rec.get("origin") or "")
        stats_start = max(fetch_start, origin.astimezone(self.tz)) if origin is not None else fetch_start
        last_hour = P.floor_hour(batch.data_end - batch.interval_length)
        first_read = P.meter_first_read(batch.inventory, batch.meter)
        gap_start = max(fetch_start, first_read) if first_read is not None else fetch_start
        self._resolve_gaps(P.unexpected_gaps(batch, gap_start, last_hour, self.tz), now, bkey)
        stats_ready = self.backfill_done(bkey)
        stored_rows = await self._stored_rows(location, batch.meter, fetch_start) if stats_ready else {}
        resolved, kept = await self.hass.async_add_executor_job(
            P.resolve_hours, batch, fetch_start, last_hour, self._states_by_role(location, batch.meter, stored_rows)
        )
        newest_start = batch.data_end - batch.interval_length
        month_start = P.month_start(newest_start, self.tz)
        if fetch_start > month_start:
            raise P.IncompleteDataError("Fetched window does not cover the start of the month")
        month = P.totals_since(resolved, month_start)
        if key not in self._stored["layout"]:
            self._stored["layout"][key] = {"meter": batch.meter, "mode": batch.mode, "channels": batch.channels}
        self.batch_info[key] = {
            "meter": batch.meter,
            "primary": primary,
            "mode": batch.mode,
            "channels": batch.channels,
            "first_read": _iso(first_read),
            "inventory": batch.inventory,
            "skipped_meters": batch.skipped_meters,
            "unknown_channels": batch.unknown_channels,
            "invalid_points": batch.invalid_points,
            "clamped_points": batch.clamped_points,
            "interval_minutes": batch.interval_length.total_seconds() / 60,
            "fetched_from": fetch_start.isoformat(),
            "data_end": batch.data_end.isoformat(),
            "kept_recorded_hours": [h.isoformat() for h in kept][-12:],
            "incomplete_hours": {
                r: sorted(h.isoformat() for h in batch.coverage.get(r, {}) if not batch.hour_complete(r, h))[-12:]
                for r in batch.sources
            },
        }
        if batch.unknown_channels:
            _LOGGER.debug("Ignoring SmartHub channels with unknown flow direction: %s", batch.unknown_channels)

        if stats_ready:
            await self._write_statistics(location, batch, resolved, stats_start, last_hour, stored_rows)
        else:
            self.async_start_backfill(location, batch.meter)

        self._stored["data_end"][key] = batch.data_end.isoformat()
        entry = {
            "location": location,
            "primary": primary,
            "meter": batch.meter,
            "mode": batch.mode,
            "month_start": month_start,
            "import_mtd": round(month.get(P.ROLE_IMPORT, 0) / P.MICRO, 3),
            "export_mtd": round(month[P.ROLE_EXPORT] / P.MICRO, 3) if P.ROLE_EXPORT in month else None,
            "net_mtd": round(month.get(P.ROLE_NET, month.get(P.ROLE_IMPORT, 0)) / P.MICRO, 3),
            "data_end": batch.data_end,
            "stale": False,
        }
        self._stored["snapshot"][key] = {
            "description": location.description, "provider": location.provider,
            "meter": entry["meter"], "mode": entry["mode"], "month_start": _iso(entry["month_start"]),
            "import_mtd": entry["import_mtd"], "export_mtd": entry["export_mtd"], "net_mtd": entry["net_mtd"],
            "data_end": _iso(entry["data_end"]),
        }
        return entry

    async def _daily_crosscheck(
        self, location: SmartHubLocation, batch: P.UsageBatch, start: datetime, now: datetime
    ) -> None:
        """Compare completed local days with the utility's DAILY totals (diagnostics only)."""
        try:
            day0 = datetime(start.astimezone(self.tz).year, start.astimezone(self.tz).month,
                            start.astimezone(self.tz).day, tzinfo=self.tz)
            payload = await self._api_call(self.api.fetch_usage, location, Aggregation.DAILY, day0, now)
            entry = P.find_usage_entry(payload)
            series = {s.get("name"): s.get("data") or [] for s in (entry or {}).get("series") or []}
            residuals = {}
            for role in batch.sources:
                hourly_by_day: dict[str, int] = {}
                for k, v in batch.intervals[role].items():
                    d = k.astimezone(self.tz).date().isoformat()
                    hourly_by_day[d] = hourly_by_day.get(d, 0) + v
                for p in series.get(batch.channels[role], []):
                    day = P.relabel_wall_clock(p["x"], self.tz)
                    nxt = datetime.combine((day + timedelta(days=1)).date(), datetime.min.time(), tzinfo=self.tz)
                    if nxt > batch.data_end or day < day0:
                        continue  # incomplete day
                    daily = P.to_micro(p.get("y") or 0) or 0
                    if role == P.ROLE_EXPORT:
                        daily = abs(daily)
                    diff = (daily - hourly_by_day.get(day.date().isoformat(), 0)) / P.MICRO
                    if abs(diff) >= 0.05:
                        residuals.setdefault(role, {})[day.date().isoformat()] = round(diff, 3)
            self.daily_check[location.id] = {"checked_from": day0.date().isoformat(),
                                             "days_off_by_0.05kWh_or_more": residuals}
        except Exception as err:  # noqa: BLE001 - diagnostics only
            self.daily_check[location.id] = {"error": f"{type(err).__name__}: {err}"}

    # -------------------------------------------------------------------- gaps
    def _resolve_gaps(
        self, gaps: list[datetime], now: datetime, mkey: str, *, confirmed: set[datetime] | None = None
    ) -> None:
        """Fail closed on holes in the utility data unless they are confirmed persistent.

        A hole is accepted once seen again >= GAP_CONFIRM later (or in an immediate
        re-fetch during backfill, passed via ``confirmed``); transient truncation is
        never written as missing energy.
        """
        store = self._stored["gaps"]
        pending, accepted = store["pending"], store["accepted"]
        prefix = f"{mkey}|"
        current = {prefix + g.isoformat() for g in gaps}
        for iso in [k for k in pending if k.startswith(prefix) and k not in current]:
            pending.pop(iso)  # filled in by the utility
        unresolved = []
        for gap in gaps:
            iso = prefix + gap.isoformat()
            if iso in accepted:
                continue
            first_seen = dt_util.parse_datetime(pending.get(iso) or "")
            if (confirmed is not None and gap in confirmed) or (first_seen and now - first_seen >= GAP_CONFIRM):
                accepted[iso] = now.isoformat()
                pending.pop(iso, None)
                _LOGGER.info("SmartHub data has a persistent gap at %s; recording it as-is", iso)
            else:
                pending.setdefault(iso, now.isoformat())
                unresolved.append(gap)
        cutoff = now - timedelta(days=BACKFILL_MAX_DAYS + 31)
        for iso in [k for k in accepted if (dt_util.parse_datetime(k.split("|")[-1]) or now) < cutoff]:
            accepted.pop(iso)
        self._save()
        if unresolved:
            raise P.IncompleteDataError(
                f"{len(unresolved)} hour(s) with missing/conflicting intervals, first {unresolved[0].isoformat()};"
                " waiting to confirm before writing"
            )

    def _accepted_gap_hours(self, mkey: str) -> set[datetime]:
        prefix = f"{mkey}|"
        return {h for h in (dt_util.parse_datetime(k[len(prefix):]) for k in self._stored["gaps"]["accepted"]
                            if k.startswith(prefix)) if h}

    # ------------------------------------------------------------- statistics
    def _metadata(self, location: SmartHubLocation, role: str, meter: str) -> StatisticMetaData:
        return StatisticMetaData(
            mean_type=StatisticMeanType.NONE,
            has_sum=True,
            name=(f"SmartHub {_ROLE_LABEL[role]} - {self.account_id} {location.description or location.id} ({meter})"
                  if self._is_primary(location.id, meter) else
                  f"SmartHub Meter {meter} {_METER_ROLE[role].title()} - {self.account_id} "
                  f"{location.description or location.id}"),
            source=DOMAIN,
            statistic_id=self._sid(role, location.id, meter),
            unit_class=EnergyConverter.UNIT_CLASS,
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        )

    async def _rows(self, ids: set[str], start: datetime, end: datetime | None) -> dict[str, list[dict]]:
        return await get_instance(self.hass).async_add_executor_job(
            statistics_during_period, self.hass, start, end, ids, "hour", None, {"sum", "state"}
        )

    async def _last_row_start(self, sid: str) -> datetime | None:
        rows = await get_instance(self.hass).async_add_executor_job(
            get_last_statistics, self.hass, 1, sid, False, {"sum"}
        )
        series = rows.get(sid) or []
        return dt_util.utc_from_timestamp(series[0]["start"]) if series else None

    async def _first_row_start(self, sid: str, now: datetime) -> datetime | None:
        days = await get_instance(self.hass).async_add_executor_job(
            statistics_during_period, self.hass, now - timedelta(days=BACKFILL_MAX_DAYS + 31), None,
            {sid}, "day", None, {"sum"},
        )
        series = days.get(sid) or []
        if not series:
            return None
        day = dt_util.utc_from_timestamp(series[0]["start"])
        hours = await self._rows({sid}, day - timedelta(days=1), day + timedelta(days=2))
        rows = hours.get(sid) or []
        return dt_util.utc_from_timestamp(rows[0]["start"]) if rows else None

    async def _stored_rows(self, location: SmartHubLocation, meter: str, start: datetime) -> dict[str, list[dict]]:
        ids = {self._sid(r, location.id, meter) for r in ROLES}
        return await self._rows(ids, P.floor_hour(start) - timedelta(hours=1), None)

    def _states_by_role(
        self, location: SmartHubLocation, meter: str, stored_rows: dict[str, list[dict]]
    ) -> dict[str, dict[datetime, int]]:
        out = {}
        for role in ROLES:
            series = stored_rows.get(self._sid(role, location.id, meter)) or []
            out[role] = {dt_util.utc_from_timestamp(r["start"]): round((r.get("state") or 0) * P.MICRO)
                         for r in series}
        return out

    async def _write_statistics(
        self,
        location: SmartHubLocation,
        batch: P.UsageBatch,
        resolved: dict[str, list[tuple[datetime, int]]],
        window_start: datetime,
        last_hour: datetime,
        stored_rows: dict[str, list[dict]],
        *,
        from_zero: bool = False,
    ) -> dict[str, tuple[int, int]]:
        """Write resolved hourly rows from ``window_start`` to ``last_hour`` as one contiguous chain.

        Returns {statistic_id: (rows written, final sum micro-kWh)}.
        """
        first_hour = P.floor_hour(window_start)
        ids = {r: self._sid(r, location.id, batch.meter) for r in resolved}
        key = f"{location.id}_{batch.meter}"
        written: dict[str, tuple[int, int]] = {}
        async with self._stats_lock:
            bases: dict[str, int] = {}
            for role, sid in ids.items():
                by_start = {dt_util.utc_from_timestamp(r["start"]): r for r in stored_rows.get(sid) or []}
                if by_start and max(by_start) > last_hour:
                    raise P.IncompleteDataError(
                        f"{sid}: newest hour {last_hour.isoformat()} older than stored {max(by_start).isoformat()}"
                    )
                if from_zero:
                    bases[sid] = 0
                    continue
                pred = by_start.get(first_hour - timedelta(hours=1))
                origin = dt_util.parse_datetime(self._backfill_record(key).get("data_start") or "")
                if pred is not None and pred.get("sum") is not None:
                    bases[sid] = round(pred["sum"] * P.MICRO)
                elif origin is not None and P.floor_hour(origin) >= first_hour:
                    bases[sid] = 0
                else:
                    # The chain is broken; rebuild from the origin rather than guess.
                    self._stored["backfill"].setdefault(key, {})["done"] = False
                    self._stored["backfill"][key]["reason"] = f"missing predecessor for {sid} at {first_hour}"
                    self._save()
                    raise P.IncompleteDataError(f"{sid}: no statistic row at {first_hour - timedelta(hours=1)}")
            planned: dict[str, list[tuple[datetime, int, int]]] = {}
            for role, sid in ids.items():
                rows = [(h, v) for h, v in resolved[role] if first_hour <= h <= last_hour]
                old_state = {dt_util.utc_from_timestamp(r["start"]): round((r.get("state") or 0) * P.MICRO)
                             for r in stored_rows.get(sid) or []}
                if role != P.ROLE_NET and (bad := P.lowering_incomplete_hours(batch, role, rows, old_state)):
                    self.counters["refused_writes"] += 1  # invariant: resolve_hours should prevent this
                    raise P.IncompleteDataError(
                        f"{sid}: incomplete data would lower {len(bad)} recorded hour(s), first {bad[0].isoformat()}"
                    )
                planned[sid] = P.chain(rows, bases[sid])
            for role, sid in ids.items():
                stats = [
                    StatisticData(start=start, state=value / P.MICRO, sum=total / P.MICRO)
                    for start, value, total in planned[sid]
                ]
                async_add_external_statistics(self.hass, self._metadata(location, role, batch.meter), stats)
                self.counters["stat_rows_written"] += len(stats)
                self.last_written[sid] = f"{len(stats)} rows {first_hour.isoformat()}..{last_hour.isoformat()}"
                written[sid] = (len(stats), planned[sid][-1][2] if planned[sid] else 0)
            await get_instance(self.hass).async_block_till_done()
        _LOGGER.debug("Wrote SmartHub statistics %s from %s to %s", sorted(ids.values()), first_hour, last_hour)
        return written

    # --------------------------------------------------------------- backfill
    @callback
    def async_start_backfill(self, location: SmartHubLocation, meter: str) -> None:
        key = f"{location.id}_{meter}"
        if self.backfill_done(key) or (key in self._backfill_tasks and not self._backfill_tasks[key].done()):
            return
        self.backfill_status[key] = "scheduled"

        async def _run() -> None:
            if self.hass.state is not CoreState.running:
                started = asyncio.Event()
                unsub = self.hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, lambda _e: started.set())
                try:
                    await started.wait()
                finally:
                    if not started.is_set():
                        unsub()
            await self._backfill(location, meter)

        self._backfill_tasks[key] = self.config_entry.async_create_background_task(
            self.hass, _run(), f"{DOMAIN} statistics backfill {key}"
        )

    async def _backfill_origin(self, location: SmartHubLocation, meter: str, now: datetime) -> datetime:
        """Same origin on every replay: existing new rows, else legacy history, else 90 days."""
        key = f"{location.id}_{meter}"
        if stored := dt_util.parse_datetime(self._backfill_record(key).get("origin") or ""):
            return stored.astimezone(self.tz)
        existing = await self._first_row_start(self._sid(P.ROLE_IMPORT, location.id, meter), now)
        legacy = await self._first_row_start(legacy_statistic_id(self.account_id, location.id), now)
        candidates = [t for t in (existing, legacy) if t is not None]
        # Earliest coverage wins, so a shortened new stream can never become the origin.
        first = min(candidates) if candidates else now - timedelta(days=HISTORICAL_IMPORT_DAYS)
        cap = now - timedelta(days=BACKFILL_MAX_DAYS)
        if first < cap:
            _LOGGER.warning("SmartHub backfill limited to %d days (history starts %s)", BACKFILL_MAX_DAYS, first.date())
            first = cap
        # Legacy rows carry local wall-clock labels stored as UTC, so their local
        # date is up to a day early; starting at that midnight covers all of it.
        local = first.astimezone(self.tz)
        return datetime(local.year, local.month, local.day, tzinfo=self.tz)

    async def _backfill(self, location: SmartHubLocation, meter: str) -> None:
        key = f"{location.id}_{meter}"
        for attempt in range(1, BACKFILL_RETRIES + 1):
            try:
                self.backfill_status[key] = f"running (attempt {attempt})"
                now = dt_util.utcnow()
                origin = await self._backfill_origin(location, meter, now)
                self._stored["backfill"][key] = {
                    "done": False, "origin": origin.isoformat(), "tz": str(self.tz), "schema": SCHEMA,
                    "meter": meter,
                }
                self._save()
                # The utility reports when each meter's readings begin (minReadDate is
                # max(window start, first read)); history before that provably doesn't exist.
                daily = await self._fetch_payload(location, origin, now, Aggregation.DAILY)
                entry = P.find_usage_entry(daily)
                if entry is None or meter not in {r["meter"] for r in P.meter_inventory(entry)}:
                    raise P.IncompleteDataError(f"Meter {meter} not listed for {origin.date()}..{now.date()}")
                first_read = P.meter_first_read(P.meter_inventory(entry), meter)
                batch: P.UsageBatch | None = None
                data_start = max(origin, P.floor_hour(first_read).astimezone(self.tz)) if first_read else origin
                if data_start > origin:
                    await self._assert_nothing_recorded(location, meter, origin, data_start)
                chunk_start = data_start
                chunks = 0
                while chunk_start < now:
                    chunk_end = min(chunk_start + timedelta(days=BACKFILL_CHUNK_DAYS), now)
                    last = chunk_end >= now
                    part = await self._fetch_chunk(location, meter, chunk_start, chunk_end, last, now,
                                                   allow_empty=batch is None)
                    if part is None:
                        # Twice-confirmed empty before any data: the meter has no history yet,
                        # unless energy was already recorded there (then fail closed).
                        await self._assert_nothing_recorded(location, meter, chunk_start, chunk_end)
                        data_start = chunk_end
                    else:
                        if part.meter != meter:
                            raise P.IncompleteDataError(f"Backfill meter changed {meter} -> {part.meter}")
                        batch = part if batch is None else batch.merge(part)
                    chunks += 1
                    self.backfill_status[key] = f"running: {chunks} chunk(s) to {chunk_end.date()}"
                    chunk_start = chunk_end
                    if not last:
                        await asyncio.sleep(BACKFILL_CHUNK_PAUSE)
                if batch is None:
                    raise P.IncompleteDataError("Backfill returned no data")
                batch.truncate_to_common_end()
                last_hour = P.floor_hour(batch.data_end - batch.interval_length)
                self._stored["backfill"][key]["data_start"] = data_start.isoformat()
                self._save()
                # A replay keeps fuller previously recorded hours (never erases energy).
                stored_rows = await self._stored_rows(location, meter, data_start)
                resolved, _kept = await self.hass.async_add_executor_job(
                    P.resolve_hours, batch, data_start, last_hour,
                    self._states_by_role(location, meter, stored_rows),
                )
                origin = data_start
                written = await self._write_statistics(
                    location, batch, resolved, origin, last_hour, stored_rows, from_zero=True
                )
                await self._verify_backfill(written, origin)
                self._stored["backfill"][key].update({
                    "done": True, "end": batch.data_end.isoformat(), "chunks": chunks,
                    "completed": dt_util.utcnow().isoformat(),
                    "rows": {sid: n for sid, (n, _s) in written.items()},
                })
                dkey = data_key(location.id, meter, self._is_primary(location.id, meter))
                self._stored["data_end"][dkey] = max(
                    batch.data_end, self._known_end(dkey) or batch.data_end
                ).isoformat()
                self._save()
                self.backfill_status[key] = (
                    f"done: {chunks} chunk(s) {origin.date()}..{batch.data_end.isoformat()}"
                )
                _LOGGER.info("SmartHub statistics backfill for %s complete (%d chunks from %s)",
                             key, chunks, origin.date())
                return
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                self.backfill_status[key] = f"failed attempt {attempt}: {type(err).__name__}: {err}"
                _LOGGER.warning("SmartHub statistics backfill attempt %d failed: %s", attempt, err)
                if attempt < BACKFILL_RETRIES:
                    delay = BACKFILL_RETRY_DELAY.total_seconds()
                    not_before = getattr(self.api, "not_before", None)
                    if not_before is not None:
                        delay = max(delay, (not_before - dt_util.utcnow()).total_seconds() + 30)
                    await asyncio.sleep(delay)
        _LOGGER.error("SmartHub statistics backfill for %s gave up; will retry on the next full update", key)

    async def _assert_nothing_recorded(
        self, location: SmartHubLocation, meter: str, start: datetime, end: datetime
    ) -> None:
        ids = {self._sid(r, location.id, meter) for r in (P.ROLE_IMPORT, P.ROLE_EXPORT)}
        rows = await self._rows(ids, P.floor_hour(start), P.floor_hour(end))
        recorded = sum(abs(r.get("state") or 0) for series in rows.values() for r in series)
        if recorded > 0:
            raise P.IncompleteDataError(
                f"Utility returned no data for {start.date()}..{end.date()} but {recorded:.2f} kWh is recorded there"
            )

    async def _fetch_chunk(
        self, location: SmartHubLocation, meter: str, start: datetime, end: datetime, last: bool, now: datetime,
        *, allow_empty: bool,
    ) -> P.UsageBatch | None:
        """Fetch one backfill chunk; holes and emptiness must reproduce on an immediate re-fetch.

        Returns None only for a chunk that is well-formed and empty twice while no
        earlier chunk had data (proven absence of history). Coverage is checked from
        ``start``, not from the first returned reading, so a truncated prefix is caught.
        """
        window_end = _FAR_FUTURE if last else end

        async def attempt() -> tuple[P.UsageBatch | None, list[datetime] | None, Exception | None]:
            try:
                part = await self._fetch_batch(location, start, end, window_end=window_end, truncate=last,
                                               meter=meter)
            except P.NoReadingsError as err:
                return None, None, err
            last_hour = P.floor_hour(part.data_end - part.interval_length) if last else P.floor_hour(end)
            return part, P.unexpected_gaps(part, start, last_hour, self.tz), None

        part, gaps, err = await attempt()
        mkey = f"{location.id}_{meter}"
        if part is not None and (not gaps or set(gaps) <= self._accepted_gap_hours(mkey)):
            return part
        await asyncio.sleep(GAP_REFETCH_PAUSE)
        part2, gaps2, err2 = await attempt()
        if part2 is None:
            if part is None and allow_empty:
                return None
            raise err2 or P.IncompleteDataError("Backfill chunk became empty on re-fetch")
        confirmed = set(gaps or []) & set(gaps2 or []) if part is not None else set()
        self._resolve_gaps(gaps2 or [], now, mkey, confirmed=confirmed)
        return part2

    async def _verify_backfill(self, written: dict[str, tuple[int, int]], origin: datetime) -> None:
        """Read back the committed rows: count, contiguity and final sum per stream.

        The recorder may still be running the import when its commit future resolves
        (it returns no future once the queue looks empty), so poll until the data is there.
        """
        deadline = asyncio.get_running_loop().time() + VERIFY_TIMEOUT
        while True:
            await get_instance(self.hass).async_block_till_done()
            rows = await self._rows(set(written), P.floor_hour(origin), None)
            problems = []
            for sid, (count, final_sum) in written.items():
                series = rows.get(sid) or []
                starts = [r["start"] for r in series]
                if len(series) != count or any(b - a != 3600 for a, b in zip(starts, starts[1:])):
                    problems.append(f"{sid}: read back {len(series)} rows, expected {count} contiguous")
                elif abs(round((series[-1].get("sum") or 0) * P.MICRO) - final_sum) > 10:
                    problems.append(f"{sid}: final sum mismatch after backfill")
            if not problems:
                return
            if asyncio.get_running_loop().time() >= deadline:
                raise P.IncompleteDataError("; ".join(problems))
            await asyncio.sleep(1)

    async def async_shutdown_api(self) -> None:
        for task in list(self._backfill_tasks.values()):
            task.cancel()
        for task in list(self._backfill_tasks.values()):
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        await self.async_shutdown()
        await self._store.async_save(self._stored)
        await self.api.close()

    # ------------------------------------------------------------ diagnostics
    def diagnostics(self) -> dict[str, Any]:
        return {
            "timezone": str(self.tz),
            "configured_meter": self.configured_meter,
            "pinned_meters": self._stored.get("meter"),
            "fixed_interval": str(self.fixed_interval) if self.fixed_interval else None,
            "schedule": None if self.schedule is None else {
                "mode": self.schedule.mode,
                "wait": str(self.schedule.wait),
                "expected_arrival": _iso(self.schedule.expected_arrival),
            },
            "consecutive_failures": self.consecutive_failures,
            "last_error": self.last_error,
            "last_success": _iso(self.last_success),
            "counters": dict(self.counters),
            "batches": self.batch_info,
            "daily_check": self.daily_check,
            "backfill": {"status": self.backfill_status, "stored": self._stored.get("backfill")},
            "data_end": self._stored.get("data_end"),
            "last_full": self._stored.get("last_full"),
            "arrivals": self._stored.get("arrivals"),
            "gaps": self._stored.get("gaps"),
            "statistics_last_written": self.last_written,
            "excluded_meters": sorted(self.excluded_meters),
            "meter_errors": self.meter_errors,
        }

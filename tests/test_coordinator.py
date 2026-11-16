"""Coordinator + recorder tests with a synthetic utility (see smarthub_fakes)."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import statistics_during_period
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.smarthub import coordinator as coord_mod
from custom_components.smarthub import processing as P
from custom_components.smarthub.api import Aggregation, SmartHubAPI
from custom_components.smarthub.const import DOMAIN
from custom_components.smarthub.exceptions import SmartHubAuthenticationError, SmartHubConnectionError

from tests.smarthub_fakes import (
    LOCATION, NET_METER, SECOND_METER, TZ, UTC, FakeUtility, consumption, generation, production,
)

NOW = "2026-11-16T18:00:00+00:00"
DATA_START = datetime(2026, 10, 20)
DATA_END = datetime(2026, 11, 16, 2, 30)       # last interval 02:15-02:30 local
SECOND_START = datetime(2026, 10, 27)
ACCOUNT = "9999"


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(coord_mod, "BACKFILL_CHUNK_PAUSE", 0)
    monkeypatch.setattr(coord_mod, "GAP_REFETCH_PAUSE", 0)
    monkeypatch.setattr(coord_mod, "BACKFILL_RETRY_DELAY", timedelta(0))
    monkeypatch.setattr(coord_mod, "BACKFILL_RETRIES", 2)


class Clock:
    """Coordinator-only clock (the recorder and asyncio keep real time)."""

    def __init__(self, real):
        self._real = real
        self.now = datetime.fromisoformat(NOW)

    def utcnow(self):
        return self.now

    def tick(self, delta):
        self.now += delta

    def __getattr__(self, name):
        return getattr(self._real, name)


@pytest.fixture
def clock(monkeypatch):
    shim = Clock(coord_mod.dt_util)
    monkeypatch.setattr(coord_mod, "dt_util", shim)
    return shim


@pytest.fixture
async def env(hass, clock):
    await hass.config.async_set_time_zone("America/Chicago")
    entry = MockConfigEntry(domain=DOMAIN, data={"account_id": ACCOUNT, "host": "x", "email": "e", "password": "p"},
                            entry_id="TESTENTRY")
    entry.add_to_hass(hass)
    api = FakeUtility(DATA_START, DATA_END, SECOND_START)
    coord = coord_mod.SmartHubDataUpdateCoordinator(hass, api, entry, TZ)
    await coord.async_load_store()
    yield hass, coord, api, entry
    for task in coord._backfill_tasks.values():
        task.cancel()


def sid(role, meter=NET_METER, primary=True):
    return coord_mod.statistic_id(role, ACCOUNT, LOCATION.id, meter, primary)


async def rows(hass, ids, start=datetime(2026, 10, 1, tzinfo=UTC)):
    return await get_instance(hass).async_add_executor_job(
        statistics_during_period, hass, start, None, set(ids), "hour", None, {"sum", "state"})


async def refresh(coord):
    coord.data = await coord._async_update_data()
    for task in list(coord._backfill_tasks.values()):
        task.cancel()
    return coord.data


async def backfilled(env):
    hass, coord, api, _entry = env
    data = await refresh(coord)
    await coord._backfill(LOCATION, NET_METER)
    await coord._backfill(LOCATION, SECOND_METER)
    return data


def force_full(coord):
    coord._stored["last_full"][LOCATION.id] = (datetime.fromisoformat(NOW) - timedelta(days=2)).isoformat()


async def test_first_refresh_month_to_date(env):
    hass, coord, api, _ = env
    data = await refresh(coord)
    nov = datetime(2026, 11, 1)
    e = data[LOCATION.id]
    imp, exp = api.total(consumption, nov), -api.total(generation, nov)
    assert (e["meter"], e["mode"], e["primary"]) == (NET_METER, P.MODE_REGISTERS, True)
    assert e["import_mtd"] == pytest.approx(imp, abs=1e-3)
    assert e["export_mtd"] == pytest.approx(exp, abs=1e-3)
    assert e["net_mtd"] == pytest.approx(imp - exp, abs=1e-3)
    assert e["month_start"] == datetime(2026, 11, 1, tzinfo=TZ)
    assert e["data_end"].astimezone(TZ) == DATA_END.replace(tzinfo=TZ)
    second = data[f"{LOCATION.id}:{SECOND_METER}"]
    assert second["primary"] is False and second["export_mtd"] is None
    assert second["import_mtd"] == pytest.approx(api.total(production, nov), abs=1e-3)
    assert coord._stored["meter"][LOCATION.id] == NET_METER


async def test_backfill_contiguous_dst_and_consistent(env):
    hass, coord, api, _ = env
    data = await backfilled(env)
    r = await rows(hass, [sid("import"), sid("export"), sid("net"), sid("import", SECOND_METER, False)])
    imp, exp, net = r[sid("import")], r[sid("export")], r[sid("net")]
    starts = [x["start"] for x in imp]
    assert all(b - a == 3600 for a, b in zip(starts, starts[1:]))
    assert datetime.fromtimestamp(starts[0], UTC).astimezone(TZ) == DATA_START.replace(tzinfo=TZ)
    assert all(b["sum"] >= a["sum"] - 1e-9 for s in (imp, exp) for a, b in zip(s, s[1:]))
    assert net[-1]["sum"] == pytest.approx(imp[-1]["sum"] - exp[-1]["sum"], abs=1e-6)
    assert imp[-1]["sum"] == pytest.approx(api.total(consumption, DATA_START), abs=1e-6)
    # fall-back day has 25 hourly rows; the collapsed repeated hour is recorded as 0, never invented
    d0, d1 = datetime(2026, 11, 1, tzinfo=TZ).timestamp(), datetime(2026, 11, 2, tzinfo=TZ).timestamp()
    day = [x for x in imp if d0 <= x["start"] < d1]
    assert len(day) == 25
    assert sum(x["state"] for x in day) == pytest.approx(
        api.total(consumption, datetime(2026, 11, 1), datetime(2026, 11, 2)), abs=1e-6)
    # stats month == sensor month
    nov = datetime(2026, 11, 1, tzinfo=TZ).timestamp()
    before = [x for x in imp if x["start"] < nov][-1]["sum"]
    assert imp[-1]["sum"] - before == pytest.approx(data[LOCATION.id]["import_mtd"], abs=1e-3)
    # the second meter starts at its own first reading and uses direction-named ids
    second = r[sid("import", SECOND_METER, False)]
    assert datetime.fromtimestamp(second[0]["start"], UTC).astimezone(TZ) == SECOND_START.replace(tzinfo=TZ)
    assert second[-1]["sum"] == pytest.approx(api.total(production, SECOND_START), abs=1e-6)
    assert sid("import", SECOND_METER, False).startswith(f"{DOMAIN}:meter_forward_")
    assert not (await rows(hass, [sid("export", SECOND_METER, False)]))


async def test_reconciliation_idempotent_revision_and_probe(env):
    hass, coord, api, _ = env
    await backfilled(env)
    before = (await rows(hass, [sid("import")]))[sid("import")]
    force_full(coord)
    data = await refresh(coord)
    after = (await rows(hass, [sid("import")]))[sid("import")]
    assert len(after) == len(before) and after[-1]["sum"] == pytest.approx(before[-1]["sum"])
    # probe only: nothing new -> identical object
    api.calls.clear()
    assert await coord._async_update_data() is data and len(api.calls) == 1
    # a complete hour revised upward propagates through later sums and the MTD sensor
    t = datetime(2026, 11, 12, 12)
    api.edit = {t: consumption(t) + 1.0}
    force_full(coord)
    data2 = await refresh(coord)
    rev = (await rows(hass, [sid("import")]))[sid("import")]
    assert rev[-1]["sum"] == pytest.approx(before[-1]["sum"] + 1.0)
    assert data2[LOCATION.id]["import_mtd"] == pytest.approx(data[LOCATION.id]["import_mtd"] + 1.0, abs=1e-3)


async def test_holes_refused_until_confirmed_and_never_erase(env, clock):
    hass, coord, api, _ = env
    await backfilled(env)
    before = (await rows(hass, [sid("import")]))[sid("import")]
    api.drop = {datetime(2026, 11, 12, 18, 15)}
    force_full(coord)
    stale = await refresh(coord)
    assert stale[LOCATION.id]["stale"] and "missing/conflicting" in coord.last_error
    clock.tick(timedelta(minutes=11))
    force_full(coord)
    ok = await refresh(coord)
    after = (await rows(hass, [sid("import")]))[sid("import")]
    assert not ok[LOCATION.id]["stale"]
    assert after[-1]["sum"] == pytest.approx(before[-1]["sum"])  # recorded energy kept
    assert ok[LOCATION.id]["net_mtd"] == pytest.approx(ok[LOCATION.id]["import_mtd"] - ok[LOCATION.id]["export_mtd"], abs=1e-3)


async def test_failures_keep_last_good_and_back_off(env):
    hass, coord, api, _ = env
    data = await backfilled(env)
    coord.data = data
    api.truncate_end = datetime(2026, 11, 15, 12, tzinfo=TZ)
    d1 = await coord._async_update_data()
    w1 = coord.schedule.wait
    api.truncate_end = None
    api.fail = SmartHubConnectionError("down")
    d2 = await coord._async_update_data()
    assert d1[LOCATION.id]["stale"] and d2[LOCATION.id]["net_mtd"] == data[LOCATION.id]["net_mtd"]
    assert (w1, coord.schedule.wait) == (timedelta(minutes=15), timedelta(minutes=30))
    api.fail = SmartHubAuthenticationError("expired")
    assert (await coord._async_update_data())[LOCATION.id]["stale"]
    api.fail = None
    coord.data = d2
    d3 = await coord._async_update_data()
    assert d3[LOCATION.id]["stale"] is False and coord.consecutive_failures == 0


async def test_first_refresh_errors(env):
    hass, coord, api, _ = env
    api.fail = SmartHubConnectionError("boom")
    with pytest.raises(UpdateFailed):
        await coord._async_update_data()
    api.fail = SmartHubAuthenticationError("bad password")
    with pytest.raises(ConfigEntryError):
        await coord._async_update_data()


async def test_layout_change_fails_closed(env):
    hass, coord, api, _ = env
    await backfilled(env)
    written = coord.counters["stat_rows_written"]
    api.drop_meter_dirs = {"REVERSE"}
    force_full(coord)
    d = await refresh(coord)
    assert d[LOCATION.id]["stale"] and "layout changed" in coord.last_error
    assert coord.counters["stat_rows_written"] == written


async def test_missing_predecessor_rebuilds_from_origin(env):
    hass, coord, api, _ = env
    await backfilled(env)
    final = (await rows(hass, [sid("import")]))[sid("import")][-1]["sum"]
    real = coord._rows

    async def without_pred(ids, start, end):
        out = await real(ids, start, end)
        return {k: v[1:] for k, v in out.items()}

    coord._rows = without_pred
    force_full(coord)
    await refresh(coord)
    key = f"{LOCATION.id}_{NET_METER}"
    assert not coord.backfill_done(key) and "missing predecessor" in coord._stored["backfill"][key]["reason"]
    coord._rows = real
    await coord._backfill(LOCATION, NET_METER)
    again = (await rows(hass, [sid("import")]))[sid("import")]
    assert coord.backfill_done(key) and again[-1]["sum"] == pytest.approx(final)


async def test_second_meter_failure_is_isolated(env):
    hass, coord, api, _ = env
    await backfilled(env)
    api.drop_second = {datetime(2026, 11, 12, 12)}
    force_full(coord)
    d = await refresh(coord)
    key = f"{LOCATION.id}:{SECOND_METER}"
    assert d[key]["stale"] and not d[LOCATION.id]["stale"] and key in coord.meter_errors
    api.drop_second = set()
    force_full(coord)
    d = await refresh(coord)
    assert not d[key]["stale"] and not coord.meter_errors


async def test_replay_with_empty_range_but_recorded_energy_fails_closed(env):
    hass, coord, api, _ = env
    await backfilled(env)
    before = [x["sum"] for x in (await rows(hass, [sid("import")]))[sid("import")]]
    key = f"{LOCATION.id}_{NET_METER}"
    coord._stored["backfill"][key] = {"origin": DATA_START.replace(tzinfo=TZ).isoformat()}
    api.empty_before = datetime(2027, 1, 1, tzinfo=TZ)
    await coord._backfill(LOCATION, NET_METER)
    assert not coord.backfill_done(key) and "recorded there" in coord.backfill_status[key]
    assert [x["sum"] for x in (await rows(hass, [sid("import")]))[sid("import")]] == before


async def test_snapshot_restore_after_restart(env):
    hass, coord, api, entry = env
    data = await backfilled(env)
    await coord._store.async_save(coord._stored)
    down = FakeUtility(DATA_START, DATA_END, SECOND_START)
    down.fail = SmartHubConnectionError("down at boot")
    restarted = coord_mod.SmartHubDataUpdateCoordinator(hass, down, entry, TZ)
    await restarted.async_load_store()
    snap = await restarted._async_update_data()
    assert snap[LOCATION.id]["stale"] and snap[LOCATION.id]["net_mtd"] == data[LOCATION.id]["net_mtd"]
    assert snap[f"{LOCATION.id}:{SECOND_METER}"]["import_mtd"] == data[f"{LOCATION.id}:{SECOND_METER}"]["import_mtd"]


async def test_throttled_login_blocks_traffic_and_schedule(env):
    hass, coord, api, _ = env

    class Resp:
        status, headers = 503, {"Retry-After": "3600"}

        async def text(self):
            return ""

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class Session:
        closed = False
        calls = 0

        def post(self, *a, **k):
            Session.calls += 1
            return Resp()

        request = post

        async def close(self):
            pass

    real = SmartHubAPI(email="e", password="p", account_id="1", timezone="UTC", mfa_totp="", host="h.invalid")

    async def session():
        return Session()

    real._get_session = session
    with pytest.raises(SmartHubConnectionError):
        await real.get_token()
    calls = Session.calls
    with pytest.raises(SmartHubConnectionError):
        await real.fetch_usage(LOCATION, Aggregation.HOURLY, datetime(2026, 11, 16, tzinfo=UTC),
                               datetime(2026, 11, 16, 1, tzinfo=UTC))
    assert Session.calls == calls and real.not_before is not None
    coord.api = real
    coord._reschedule(datetime.fromisoformat(NOW))
    assert datetime.fromisoformat(NOW) + coord.schedule.wait >= real.not_before


async def test_refresh_during_startup_does_not_wait_for_recorder_commit(env, monkeypatch):
    """The recorder commits only after Home Assistant has started: setup must not wait on it."""
    import asyncio

    from homeassistant.core import CoreState

    hass, coord, api, _ = env
    await backfilled(env)  # statistics exist, so the next full update writes rows
    never = asyncio.get_running_loop().create_future()
    monkeypatch.setattr(get_instance(hass), "async_block_till_done", lambda: never)
    hass.set_state(CoreState.starting)
    try:
        force_full(coord)
        data = await asyncio.wait_for(coord._async_update_data(), 30)
    finally:
        hass.set_state(CoreState.running)
    assert not data[LOCATION.id]["stale"] and coord.counters["stat_rows_written"] > 0

"""Synthetic SmartHub utility emulator for tests (no real account data).

Generates the "utility-usage/poll" payload shape: interval x values are the
utility-local wall clock encoded as a UTC epoch (ms); meters carry flowDirection,
seriesId and minReadDate = max(requested window start, first reading).
Meter NET_METER has FORWARD/REVERSE/NET channels (the grid connection); meter
SECOND_METER is a FORWARD-only meter whose readings start later.
"""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from custom_components.smarthub import processing as P
from custom_components.smarthub.api import SmartHubLocation

TZ = ZoneInfo("America/Chicago")
UTC = timezone.utc
NET_METER = "11111111"
SECOND_METER = "22222222"
LOCATION = SmartHubLocation(id="5000", service="electric", description="HOME", provider="TEST COOP")
STEP = timedelta(minutes=15)


def wall_epoch_ms(naive: datetime) -> int:
    return int(naive.replace(tzinfo=UTC).timestamp() * 1000)


def _exists(naive: datetime) -> bool:
    """False for wall-clock times skipped by a spring-forward transition."""
    aware = naive.replace(tzinfo=TZ)
    return aware.astimezone(UTC).astimezone(TZ).replace(tzinfo=None) == naive


def consumption(naive: datetime) -> float:
    return round(0.20 + (0.10 if naive.hour % 3 == 0 else 0.0) + (0.05 if naive.minute == 30 else 0.0), 2)


def generation(naive: datetime) -> float:
    return -0.6 if 10 <= naive.hour <= 14 else 0.0


def production(naive: datetime) -> float:
    return 0.4 if 8 <= naive.hour <= 16 else 0.0


class FakeUtility:
    """Serves synthetic data between ``data_start`` and ``data_end`` (local wall clock)."""

    def __init__(self, data_start: datetime, data_end: datetime, second_start: datetime) -> None:
        self.data_start, self.data_end, self.second_start = data_start, data_end, second_start
        self.points: list[datetime] = []
        t = data_start
        while t < data_end:
            if _exists(t):
                self.points.append(t)
            t += STEP
        self.calls: list[tuple[str, datetime, datetime]] = []
        self.fail: Exception | None = None
        self.drop: set[datetime] = set()        # consumption points omitted (naive local)
        self.drop_once: set[datetime] = set()
        self.drop_second: set[datetime] = set()
        self.edit: dict[datetime, float] = {}  # consumption overrides
        self.dup: dict[datetime, float] = {}   # conflicting duplicate consumption
        self.truncate_end: datetime | None = None
        self.empty_before: datetime | None = None
        self.drop_meter_dirs: set[str] = set()
        self.not_before = None
        self.closed = False

    async def get_service_locations(self):
        if self.fail:
            raise self.fail
        return [LOCATION]

    async def close(self):
        self.closed = True

    def _meters(self, start_local: datetime) -> list[dict]:
        first_net = max(start_local, self.data_start.replace(tzinfo=TZ))
        first_second = max(start_local, self.second_start.replace(tzinfo=TZ))
        rows = [
            (NET_METER, 1, "FORWARD", f"{NET_METER} - Consumption", True, first_net),
            (NET_METER, 2, "REVERSE", f"{NET_METER} - Generation", True, first_net),
            (NET_METER, 3, "NET", f"{NET_METER} - Net", True, first_net),
            (SECOND_METER, 1, "FORWARD", SECOND_METER, False, first_second),
        ]
        return [
            {"meterNumber": m, "channel": c, "flowDirection": d, "seriesId": s, "isNetMeter": n,
             "minReadDate": int(first.timestamp() * 1000)}
            for m, c, d, s, n, first in rows if d not in self.drop_meter_dirs
        ]

    async def fetch_usage(self, location, aggregation, start, end):
        self.calls.append((aggregation.value, start, end))
        if self.fail:
            raise self.fail
        lo = start - timedelta(hours=1)  # the real API may return one extra leading hour
        series = {f"{NET_METER} - Consumption": [], f"{NET_METER} - Generation": [], f"{NET_METER} - Net": [],
                  SECOND_METER: []}
        dropped_once = set()
        for naive in self.points:
            local = naive.replace(tzinfo=TZ)
            if not (lo <= local < end):
                continue
            if self.truncate_end is not None and local >= self.truncate_end:
                continue
            if self.empty_before is not None and local < self.empty_before:
                continue
            x = wall_epoch_ms(naive)
            c = self.edit.get(naive, consumption(naive))
            if naive in self.drop_once:
                dropped_once.add(naive)
            elif naive not in self.drop:
                series[f"{NET_METER} - Consumption"].append({"x": x, "y": c})
                if naive in self.dup:
                    series[f"{NET_METER} - Consumption"].append({"x": x, "y": self.dup[naive]})
            series[f"{NET_METER} - Generation"].append({"x": x, "y": generation(naive)})
            if naive.minute != 45:  # the NET series drops points, like the real one
                series[f"{NET_METER} - Net"].append({"x": x, "y": round(c + generation(naive), 2)})
            if naive >= self.second_start and naive not in self.drop_second:
                series[SECOND_METER].append({"x": x, "y": production(naive)})
        self.drop_once -= dropped_once
        if aggregation.value == "DAILY":
            for name, pts in series.items():
                days: dict[int, float] = {}
                for p in pts:
                    t = P.relabel_wall_clock(p["x"], TZ)
                    dx = wall_epoch_ms(datetime(t.year, t.month, t.day))
                    days[dx] = round(days.get(dx, 0) + p["y"], 2)
                series[name] = [{"x": k, "y": v} for k, v in sorted(days.items())]
        meters = self._meters(start)
        listed = {m["seriesId"] for m in meters}
        out = [{"name": name, "data": pts} for name, pts in series.items()
               if name in listed or (name == SECOND_METER and SECOND_METER in listed)]
        xs = sorted({p["x"] for pts in series.values() for p in pts})
        x2i = {str(xs[-1]): {"interval": {"start": xs[-1], "end": xs[-1] + 900_000}}} if xs else {}
        return {"status": "COMPLETE", "data": {"ELECTRIC": [{
            "type": "USAGE", "meters": meters, "series": copy.deepcopy(out), "xToOrderedInterval": x2i,
        }]}}

    # expected values -------------------------------------------------------
    def total(self, fn, since: datetime, until: datetime | None = None, *, start: datetime | None = None) -> float:
        lo = max(since, start) if start else since
        return round(sum(fn(t) for t in self.points if lo <= t and (until is None or t < until)), 6)

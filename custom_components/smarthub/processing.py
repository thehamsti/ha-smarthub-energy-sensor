"""Pure data processing for SmartHub usage payloads.

This module deliberately has no Home Assistant imports so it can be unit tested
in isolation.

SmartHub "utility-usage/poll" semantics (verified against a live NISC SmartHub deployment):
- ``series[].data[].x`` is the *utility-local wall-clock* start of each interval,
  encoded as if it were a UTC epoch (ms). ``meters[].minReadDate``/``maxReadDate``
  are true UTC instants. Interval length is given by ``xToOrderedInterval``.
- A net-metered service exposes one physical meter with FORWARD ("Consumption",
  delivered from grid), REVERSE ("Generation", received by grid, values <= 0) and
  NET channels. The NET series can drop points, so delivered/received registers
  are preferred when present.
- Energy is accumulated in integer micro-kWh to avoid float drift in running sums.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, tzinfo
import math
from statistics import median
from typing import Any, Iterable

MICRO = 1_000_000

FORWARD_DIRECTIONS = frozenset({"FORWARD", "TOTAL"})
EXPORT_DIRECTIONS = frozenset({"REVERSE", "RETURN"})
NET_DIRECTIONS = frozenset({"NET"})

ROLE_IMPORT = "import"
ROLE_EXPORT = "export"
ROLE_NET = "net"

MODE_REGISTERS = "registers"      # FORWARD + REVERSE/RETURN channels
MODE_NET_SPLIT = "net_split"      # only a NET channel: split by sign
MODE_FORWARD_ONLY = "forward_only"

DEFAULT_INTERVAL = timedelta(minutes=15)


class IncompleteDataError(Exception):
    """The payload is missing data needed for a consistent update."""


class NoReadingsError(IncompleteDataError):
    """A well-formed response whose selected series hold no readings in the window."""


@dataclass
class UsageBatch:
    """Interval data for the selected meter, keyed by interval start (UTC)."""

    meter: str
    mode: str
    channels: dict[str, str]                      # role -> seriesId
    intervals: dict[str, dict[datetime, int]]     # role -> {utc start: micro-kWh}
    interval_length: timedelta
    data_end: datetime                            # UTC end of newest common interval
    sources: tuple[str, ...] = ()               # roles taken directly from utility series
    inventory: list[dict[str, Any]] = field(default_factory=list)
    skipped_meters: list[str] = field(default_factory=list)
    unknown_channels: list[str] = field(default_factory=list)
    invalid_points: int = 0
    clamped_points: int = 0
    # role -> UTC hour -> set of interval slots present (an explicit 0 counts as present)
    coverage: dict[str, dict[datetime, set[int]]] = field(default_factory=dict)
    # role -> UTC hours that had conflicting duplicate readings
    conflicts: dict[str, set[datetime]] = field(default_factory=dict)

    @property
    def has_export(self) -> bool:
        return ROLE_EXPORT in self.intervals

    @property
    def slots_per_hour(self) -> int:
        return max(1, round(3600 / self.interval_length.total_seconds()))

    def hour_complete(self, role: str, hour: datetime) -> bool:
        """True if every interval of ``hour`` is present and unambiguous for ``role``."""
        if hour in self.conflicts.get(role, ()):
            return False
        return len(self.coverage.get(role, {}).get(hour, ())) >= self.slots_per_hour

    def truncate_to_common_end(self) -> "UsageBatch":
        """Drop intervals after the newest interval every source channel has reached."""
        source_roles = [r for r in (self.sources or tuple(self.intervals)) if self.intervals.get(r)]
        if not source_roles:
            raise IncompleteDataError("No readings")
        common = min(max(self.intervals[r]) for r in source_roles)
        for role in self.intervals:
            self.intervals[role] = {k: v for k, v in self.intervals[role].items() if k <= common}
        last_hour = floor_hour(common)
        for role in self.coverage:
            self.coverage[role] = {h: v for h, v in self.coverage[role].items() if h <= last_hour}
            if h_cov := self.coverage[role].get(last_hour):
                slot_limit = (common - last_hour) // self.interval_length
                self.coverage[role][last_hour] = {x for x in h_cov if x <= slot_limit}
        self.data_end = common + self.interval_length
        return self

    def merge(self, other: "UsageBatch") -> "UsageBatch":
        """Merge another batch (e.g. an adjacent backfill chunk) into this one."""
        if other.meter != self.meter or other.mode != self.mode:
            raise IncompleteDataError("Cannot merge batches for different meters/modes")
        for role, values in other.intervals.items():
            mine = self.intervals.setdefault(role, {})
            for k, v in values.items():
                if k in mine and mine[k] != v:
                    self.conflicts.setdefault(role, set()).add(floor_hour(k))
                mine[k] = v
        for role, hours in other.coverage.items():
            mine_cov = self.coverage.setdefault(role, {})
            for h, slots in hours.items():
                mine_cov[h] = set(mine_cov.get(h, set())) | set(slots)
        for role, hours in other.conflicts.items():
            self.conflicts.setdefault(role, set()).update(hours)
        self.data_end = max(self.data_end, other.data_end)
        self.invalid_points += other.invalid_points
        self.clamped_points += other.clamped_points
        return self


def relabel_wall_clock(x_ms: float, tz: tzinfo) -> datetime:
    """Interpret a SmartHub x value (local wall clock encoded as UTC) in ``tz``.

    Ambiguous wall times (DST fall-back) resolve to the first occurrence (fold=0).
    """
    naive = datetime.fromtimestamp(x_ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    return naive.replace(tzinfo=tz)


def to_micro(value: Any) -> int | None:
    """Convert a kWh value to integer micro-kWh, rejecting invalid values."""
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value):
        return None
    return int(round(value * MICRO))


def find_usage_entry(payload: Any) -> dict[str, Any] | None:
    """Return the ELECTRIC/USAGE entry of a poll response, if any."""
    if not isinstance(payload, dict):
        return None
    electric = (payload.get("data") or {}).get("ELECTRIC") or []
    for entry in electric:
        if isinstance(entry, dict) and entry.get("type") == "USAGE":
            return entry
    return None


def meter_inventory(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a compact, non-sensitive description of the meters in a USAGE entry."""
    out = []
    for meter in entry.get("meters") or []:
        if not isinstance(meter, dict):
            continue
        out.append({
            "meter": str(meter.get("meterNumber") or meter.get("meterName") or meter.get("seriesId") or ""),
            "channel": meter.get("channel"),
            "direction": str(meter.get("flowDirection") or "FORWARD").upper(),
            "series": meter.get("seriesId"),
            "is_net_meter": bool(meter.get("isNetMeter")),
            "min_read": meter.get("minReadDate") if isinstance(meter.get("minReadDate"), (int, float)) else None,
        })
    return out


def meter_first_read(inventory: list[dict[str, Any]], meter: str) -> datetime | None:
    """Earliest reading the utility reports for a meter (true UTC instant), if given.

    SmartHub's ``minReadDate`` is max(requested window start, first reading), so it
    only proves where a meter's history begins when the request started earlier.
    """
    reads = [r["min_read"] for r in inventory if r["meter"] == meter and r.get("min_read") is not None]
    return datetime.fromtimestamp(min(reads) / 1000, tz=timezone.utc) if reads else None


def select_meter(inventory: list[dict[str, Any]], preferred: str | None = None) -> tuple[str, str]:
    """Choose the meter to track. Returns (meter, reason).

    Order: configured meter; the single net-capable meter (NET/REVERSE/RETURN
    channel or isNetMeter); otherwise the first FORWARD/TOTAL meter.
    """
    meters: dict[str, list[dict[str, Any]]] = {}
    for row in inventory:
        meters.setdefault(row["meter"], []).append(row)
    if not meters:
        raise IncompleteDataError("No meters in usage data")
    if preferred:
        if str(preferred) in meters:
            return str(preferred), "configured"
        raise IncompleteDataError(f"Configured meter {preferred} not present")
    net_capable = [
        meter for meter, rows in meters.items()
        if any(r["is_net_meter"] or r["direction"] in NET_DIRECTIONS | EXPORT_DIRECTIONS for r in rows)
    ]
    if len(net_capable) == 1:
        return net_capable[0], "net_meter"
    for meter, rows in meters.items():
        if any(r["direction"] in FORWARD_DIRECTIONS for r in rows):
            return meter, "first_forward" if not net_capable else "ambiguous_net_first_forward"
    return next(iter(meters)), "first"


def _interval_length(entry: dict[str, Any], xs: Iterable[int]) -> timedelta:
    mapping = entry.get("xToOrderedInterval") or {}
    xs = sorted(set(xs))
    if xs:
        info = mapping.get(str(xs[-1])) or {}
        interval = info.get("interval") or {}
        start, end = interval.get("start"), interval.get("end")
        if isinstance(start, (int, float)) and isinstance(end, (int, float)) and end > start:
            return timedelta(milliseconds=end - start)
        diffs = [b - a for a, b in zip(xs, xs[1:]) if b > a]
        if diffs:
            # the most common spacing; ignore DST marker points (e.g. :57)
            common = max(set(diffs), key=diffs.count)
            return timedelta(milliseconds=common)
    return DEFAULT_INTERVAL


def build_batch(
    payload: Any,
    tz: tzinfo,
    preferred_meter: str | None = None,
    window: tuple[datetime, datetime] | None = None,
    truncate: bool = True,
) -> UsageBatch:
    """Extract interval data for the selected meter from a poll response.

    ``window`` (aware datetimes) keeps only intervals with start in [start, end);
    used to make overlapping backfill chunks idempotent.
    Raises IncompleteDataError if the payload can't produce a consistent batch.
    """
    entry = find_usage_entry(payload)
    if entry is None:
        raise IncompleteDataError("No ELECTRIC USAGE entry in response")
    inventory = meter_inventory(entry)
    meter, _reason = select_meter(inventory, preferred_meter)

    roles: dict[str, str] = {}
    unknown: list[str] = []
    for row in inventory:
        if row["meter"] != meter or not row["series"]:
            continue
        direction = row["direction"]
        if direction in FORWARD_DIRECTIONS:
            role = ROLE_IMPORT
        elif direction in EXPORT_DIRECTIONS:
            role = ROLE_EXPORT
        elif direction in NET_DIRECTIONS:
            role = ROLE_NET
        else:
            unknown.append(f"{row['meter']}/{row['channel']}:{direction}")
            continue
        roles.setdefault(role, row["series"])

    series_by_name = {
        s.get("name"): s for s in entry.get("series") or [] if isinstance(s, dict)
    }
    for role, series_id in roles.items():
        if series_id not in series_by_name:
            raise IncompleteDataError(f"Declared {role} series '{series_id}' missing from response")

    if ROLE_IMPORT in roles and ROLE_EXPORT in roles:
        mode = MODE_REGISTERS
        sources = (ROLE_IMPORT, ROLE_EXPORT)
    elif ROLE_NET in roles:
        mode = MODE_NET_SPLIT
        sources = (ROLE_NET,)
    elif ROLE_IMPORT in roles:
        mode = MODE_FORWARD_ONLY
        sources = (ROLE_IMPORT,)
    else:
        raise IncompleteDataError(f"No usable channel for meter {meter}")

    invalid = 0
    empty_roles: list[str] = []
    malformed = False
    raw: dict[str, dict[datetime, int]] = {}
    src_conflicts: dict[str, set[datetime]] = {}
    all_x: list[int] = []
    for role in sources:
        points: dict[datetime, int] = {}
        conflicts: set[datetime] = set()
        role_data = series_by_name[roles[role]].get("data")
        if not isinstance(role_data, list):
            malformed = True
            role_data = []
        invalid_before = invalid
        for point in role_data:
            if not isinstance(point, dict) or not isinstance(point.get("x"), (int, float)):
                invalid += 1
                continue
            value = to_micro(point.get("y"))
            if value is None:
                invalid += 1
                continue
            local = relabel_wall_clock(point["x"], tz)
            if window is not None and not (window[0] <= local < window[1]):
                continue
            all_x.append(int(point["x"]))
            key = local.astimezone(timezone.utc)
            if key in points and points[key] != value:
                # Conflicting duplicate: keep the larger reading but never trust the
                # hour enough to lower an already recorded value.
                conflicts.add(floor_hour(key))
                value = max(value, points[key])
            points[key] = value
        if invalid != invalid_before:
            malformed = True
        if not points:
            empty_roles.append(role)
        raw[role] = points
        src_conflicts[role] = conflicts
    if empty_roles:
        if len(empty_roles) == len(sources) and not malformed:
            raise NoReadingsError(f"No readings for meter {meter} in the requested window")
        raise IncompleteDataError(f"Missing or invalid {', '.join(empty_roles)} readings for meter {meter}")

    clamped = 0
    intervals: dict[str, dict[datetime, int]] = {}
    if mode == MODE_REGISTERS:
        imp, exp = {}, {}
        for k, v in raw[ROLE_IMPORT].items():
            if v < 0:
                clamped += 1
                v = 0
            imp[k] = v
        for k, v in raw[ROLE_EXPORT].items():
            exp[k] = abs(v)
        keys = set(imp) | set(exp)
        intervals = {
            ROLE_IMPORT: imp,
            ROLE_EXPORT: exp,
            ROLE_NET: {k: imp.get(k, 0) - exp.get(k, 0) for k in keys},
        }
    elif mode == MODE_NET_SPLIT:
        net = raw[ROLE_NET]
        intervals = {
            ROLE_IMPORT: {k: max(0, v) for k, v in net.items()},
            ROLE_EXPORT: {k: max(0, -v) for k, v in net.items()},
            ROLE_NET: dict(net),
        }
    else:
        imp = {}
        for k, v in raw[ROLE_IMPORT].items():
            if v < 0:
                clamped += 1
                v = 0
            imp[k] = v
        intervals = {ROLE_IMPORT: imp}

    length = _interval_length(entry, all_x)
    step_s = length.total_seconds()

    def _coverage(points: dict[datetime, int]) -> dict[datetime, set[int]]:
        cov: dict[datetime, set[int]] = {}
        for k in points:
            hour = floor_hour(k)
            cov.setdefault(hour, set()).add(int((k - hour).total_seconds() // step_s))
        return cov

    src_cov = {role: _coverage(raw[role]) for role in sources}
    if mode == MODE_REGISTERS:
        imp_cov, exp_cov = src_cov[ROLE_IMPORT], src_cov[ROLE_EXPORT]
        coverage = {
            ROLE_IMPORT: imp_cov,
            ROLE_EXPORT: exp_cov,
            ROLE_NET: {h: imp_cov.get(h, set()) & exp_cov.get(h, set()) for h in set(imp_cov) | set(exp_cov)},
        }
        all_conflicts = src_conflicts[ROLE_IMPORT] | src_conflicts[ROLE_EXPORT]
        conflicts_by_role = {
            ROLE_IMPORT: set(src_conflicts[ROLE_IMPORT]),
            ROLE_EXPORT: set(src_conflicts[ROLE_EXPORT]),
            ROLE_NET: all_conflicts,
        }
    else:
        only = sources[0]
        coverage = {role: src_cov[only] for role in intervals}
        conflicts_by_role = {role: set(src_conflicts[only]) for role in intervals}

    batch = UsageBatch(
        meter=meter,
        mode=mode,
        channels={r: roles[r] for r in sources},
        intervals=intervals,
        interval_length=length,
        data_end=max(max(v) for v in intervals.values() if v) + length,
        sources=sources,
        inventory=inventory,
        skipped_meters=sorted({r["meter"] for r in inventory if r["meter"] != meter}),
        unknown_channels=unknown,
        invalid_points=invalid,
        clamped_points=clamped,
        coverage=coverage,
        conflicts=conflicts_by_role,
    )
    # Only report the span every source channel has reached, so an hour is never
    # written with import but without export.
    return batch.truncate_to_common_end() if truncate else batch


def floor_hour(t: datetime) -> datetime:
    """Floor an aware datetime to the top of its UTC hour (returned in UTC)."""
    return t.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def hourly(intervals: dict[datetime, int]) -> dict[datetime, int]:
    """Sum interval values into UTC hour buckets."""
    out: dict[datetime, int] = {}
    for start, value in intervals.items():
        hour = floor_hour(start)
        out[hour] = out.get(hour, 0) + value
    return out


def contiguous_rows(hours: dict[datetime, int], first_hour: datetime, last_hour: datetime) -> list[tuple[datetime, int]]:
    """Every hour in [first_hour, last_hour], zero-filled where there is no data."""
    rows = []
    hour = floor_hour(first_hour)
    last_hour = floor_hour(last_hour)
    while hour <= last_hour:
        rows.append((hour, hours.get(hour, 0)))
        hour += timedelta(hours=1)
    return rows


def chain(rows: list[tuple[datetime, int]], base_micro: int) -> list[tuple[datetime, int, int]]:
    """Return (start, state, running sum) rows continuing from ``base_micro``."""
    total = base_micro
    out = []
    for start, value in rows:
        total += value
        out.append((start, value, total))
    return out


def unexpected_gaps(
    batch: UsageBatch,
    first_hour: datetime,
    last_hour: datetime,
    tz: tzinfo,
) -> list[datetime]:
    """Hours in [first_hour, last_hour] lacking any interval or holding conflicting data.

    Exempt: the newest (terminal) hour, which is legitimately partial until the next
    publish, and the repeated local hour at DST fall-back (fold=1), which SmartHub
    collapses into the first occurrence.
    """
    gaps = []
    hour = floor_hour(first_hour)
    last_hour = floor_hour(last_hour)
    while hour < last_hour:
        if hour.astimezone(tz).fold == 0:
            for role in batch.sources:
                if not batch.hour_complete(role, hour):
                    gaps.append(hour)
                    break
        hour += timedelta(hours=1)
    return gaps


def lowering_incomplete_hours(
    batch: UsageBatch,
    role: str,
    rows: list[tuple[datetime, int]],
    stored_state: dict[datetime, int],
    tolerance: int = 1,
) -> list[datetime]:
    """Hours where an incomplete new value would lower an already recorded value.

    Writing those would erase recorded energy based on missing data, so the caller
    must not write. Complete hours may legitimately be revised downwards.
    """
    bad = []
    for hour, value in rows:
        old = stored_state.get(hour)
        if old is not None and value < old - tolerance and not batch.hour_complete(role, hour):
            bad.append(hour)
    return bad


def resolve_hours(
    batch: UsageBatch,
    first_hour: datetime,
    last_hour: datetime,
    stored: dict[str, dict[datetime, int]] | None = None,
) -> tuple[dict[str, list[tuple[datetime, int]]], list[datetime]]:
    """Contiguous hourly values per role for [first_hour, last_hour], consistent across roles.

    For an hour that is incomplete in ``batch`` (terminal partial hour, confirmed utility
    gap, DST fold), the previously recorded import/export pair is kept whenever any of
    them would decrease, so missing data never erases recorded energy. Import and export are
    resolved together and net is always import - export of the resolved values.
    Returns (rows per role, hours where stored values were kept).
    """
    stored = stored or {}
    roles = [r for r in (ROLE_IMPORT, ROLE_EXPORT, ROLE_NET) if r in batch.intervals]
    base_roles = [r for r in (ROLE_IMPORT, ROLE_EXPORT) if r in roles]
    new_hourly = {r: hourly(batch.intervals[r]) for r in roles}
    out: dict[str, list[tuple[datetime, int]]] = {r: [] for r in roles}
    kept: list[datetime] = []
    hour = floor_hour(first_hour)
    last_hour = floor_hour(last_hour)
    while hour <= last_hour:
        new = {r: new_hourly[r].get(hour, 0) for r in roles}
        old = {r: stored.get(r, {}).get(hour) for r in roles}
        use_old = (
            any(not batch.hour_complete(s, hour) for s in batch.sources)
            and all(old[r] is not None for r in base_roles)
            and any(new[r] < old[r] for r in base_roles)
        )
        vals = {r: (old[r] if use_old else new[r]) for r in base_roles}
        if ROLE_NET in roles:
            vals[ROLE_NET] = (vals[ROLE_IMPORT] - vals[ROLE_EXPORT]) if ROLE_EXPORT in vals else (
                old[ROLE_NET] if use_old and old[ROLE_NET] is not None else new[ROLE_NET])
        for r in roles:
            out[r].append((hour, vals[r]))
        if use_old:
            kept.append(hour)
        hour += timedelta(hours=1)
    return out, kept


def totals_since(rows: dict[str, list[tuple[datetime, int]]], start: datetime) -> dict[str, int]:
    """Sum resolved hourly rows with hour >= start (aware)."""
    start_utc = floor_hour(start)
    return {r: sum(v for h, v in vals if h >= start_utc) for r, vals in rows.items()}


def month_start(t: datetime, tz: tzinfo) -> datetime:
    local = t.astimezone(tz)
    return datetime(local.year, local.month, 1, tzinfo=tz)


def month_to_date(batch: UsageBatch, tz: tzinfo, fetched_from: datetime) -> dict[str, Any]:
    """Totals for the local month of the newest reading.

    Raises IncompleteDataError if the fetched window does not cover the month start.
    """
    newest_start = batch.data_end - batch.interval_length
    start = month_start(newest_start, tz)
    if fetched_from > start:
        raise IncompleteDataError("Fetched window does not cover the start of the month")
    totals = {}
    for role, values in batch.intervals.items():
        totals[role] = sum(v for k, v in values.items() if k >= start)
    return {
        "month_start": start,
        "import": totals.get(ROLE_IMPORT, 0) / MICRO,
        "export": totals.get(ROLE_EXPORT, 0) / MICRO if ROLE_EXPORT in totals else None,
        "net": (totals.get(ROLE_NET, totals.get(ROLE_IMPORT, 0))) / MICRO,
        "data_end": batch.data_end,
    }


# ---------------------------------------------------------------------------
# Poll scheduling
# ---------------------------------------------------------------------------

FAILURE_BACKOFF_MIN = (15, 30, 60, 120, 360)
WINDOW_PROBE = timedelta(minutes=15)
WINDOW_LEAD_IN = timedelta(minutes=75)
WINDOW_TAIL = timedelta(hours=2)
MAX_IDLE = timedelta(hours=12)
LEARNING_INTERVAL = timedelta(minutes=60)
MIN_WAIT = timedelta(minutes=1)
DEFAULT_LEAD = timedelta(hours=2)
DAILY_STEP_MIN = timedelta(hours=12)


@dataclass
class Schedule:
    wait: timedelta
    mode: str
    expected_arrival: datetime | None = None


def compute_schedule(
    now: datetime,
    data_end: datetime | None,
    arrivals: list[tuple[datetime, datetime]],
    consecutive_failures: int = 0,
    fixed_interval: timedelta | None = None,
) -> Schedule:
    """Decide when to poll next.

    ``arrivals`` holds (seen_at, data_end) pairs for batches observed by a probe
    shortly after they were published. With a daily publish cadence, probing is
    concentrated around the expected publish time; otherwise poll at a fixed rate.
    """
    if consecutive_failures > 0:
        minutes = FAILURE_BACKOFF_MIN[min(consecutive_failures, len(FAILURE_BACKOFF_MIN)) - 1]
        return Schedule(timedelta(minutes=minutes), "failure_backoff")

    if fixed_interval is not None:
        return Schedule(fixed_interval, "fixed")
    ends = sorted({end for _seen, end in arrivals})
    steps = [b - a for a, b in zip(ends, ends[1:]) if b > a]
    if steps:
        # Missed batches show up as multiples of the cadence; ignore them.
        shortest = min(steps)
        steps = [x for x in steps if x <= shortest * 1.5]
    if data_end is None or not steps:
        return Schedule(LEARNING_INTERVAL, "learning")
    step = median(steps)
    if step < DAILY_STEP_MIN:
        return Schedule(LEARNING_INTERVAL, "frequent_publish")

    leads = [seen - end for seen, end in arrivals if timedelta(0) <= seen - end < step]
    lead = median(leads) if leads else DEFAULT_LEAD
    expected = data_end + step + lead
    window_start = expected - WINDOW_LEAD_IN
    window_end = expected + WINDOW_TAIL
    if now < window_start:
        wait = min(window_start - now, MAX_IDLE)
        mode = "idle_until_window"
    elif now <= window_end:
        wait = WINDOW_PROBE
        mode = "window"
    else:
        overdue = now - window_end
        if overdue < timedelta(hours=2):
            wait = timedelta(minutes=30)
        elif overdue < timedelta(hours=6):
            wait = timedelta(minutes=60)
        else:
            wait = timedelta(minutes=120)
        mode = "overdue_backoff"
    return Schedule(max(wait, MIN_WAIT), mode, expected)

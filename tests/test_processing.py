"""Unit tests for custom_components/smarthub/processing.py (pure Python, synthetic data only)."""
from __future__ import annotations

import json
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
COMPONENT = os.environ.get("SMARTHUB_COMPONENT", os.path.join(HERE, "..", "custom_components", "smarthub"))
sys.path.insert(0, os.path.abspath(COMPONENT))
import processing as P  # noqa: E402

DEN = ZoneInfo("America/Chicago")  # any US zone with 2026 DST on Mar 8 / Nov 1
UTC = timezone.utc


def wall_ms(y, mo, d, h=0, mi=0):
    """x value: local wall clock encoded as a UTC epoch (ms)."""
    return int(datetime(y, mo, d, h, mi, tzinfo=UTC).timestamp() * 1000)


def payload(points, meters=None, net_gaps=(), interval_ms=900_000):
    """points: list of (x_ms, consumption, generation(<=0), other_meter)."""
    meters = meters or [
        {"meterNumber": "M1", "channel": 1, "flowDirection": "FORWARD", "seriesId": "M1 - Consumption", "isNetMeter": True},
        {"meterNumber": "M1", "channel": 2, "flowDirection": "REVERSE", "seriesId": "M1 - Generation", "isNetMeter": True},
        {"meterNumber": "M1", "channel": 3, "flowDirection": "NET", "seriesId": "M1 - Net", "isNetMeter": True},
        {"meterNumber": "M2", "channel": 1, "flowDirection": "FORWARD", "seriesId": "M2", "isNetMeter": False},
    ]
    series = [
        {"name": "M2", "data": [{"x": x, "y": o} for x, _c, _g, o in points]},
        {"name": "M1 - Consumption", "data": [{"x": x, "y": c} for x, c, _g, _o in points]},
        {"name": "M1 - Generation", "data": [{"x": x, "y": g} for x, _c, g, _o in points]},
        {"name": "M1 - Net", "data": [{"x": x, "y": round(c + g, 2)} for i, (x, c, g, _o) in enumerate(points) if i not in net_gaps]},
    ]
    last = points[-1][0]
    return {"status": "COMPLETE", "data": {"ELECTRIC": [{
        "type": "USAGE", "meters": meters, "series": series,
        "xToOrderedInterval": {str(last): {"interval": {"start": last, "end": last + interval_ms}}},
    }]}}


def day_points(y, mo, d, hours=24, cons=0.25, gen_at_noon=-0.5):
    pts = []
    for h in range(hours):
        for q in range(4):
            g = gen_at_noon if 10 <= h <= 14 else 0.0
            pts.append((wall_ms(y, mo, d, h, q * 15), cons, g, 0.1))
    return pts


class MeterAndChannels(unittest.TestCase):
    def test_selects_net_meter_and_skips_other(self):
        b = P.build_batch(payload(day_points(2026, 11, 10)), DEN)
        self.assertEqual(b.meter, "M1")
        self.assertEqual(b.mode, P.MODE_REGISTERS)
        self.assertEqual(b.skipped_meters, ["M2"])
        self.assertEqual(b.channels, {"import": "M1 - Consumption", "export": "M1 - Generation"})

    def test_reverse_is_positive_export_and_net_from_registers_ignores_net_gaps(self):
        b = P.build_batch(payload(day_points(2026, 11, 10), net_gaps={5, 50}), DEN)
        self.assertEqual(sum(b.intervals["import"].values()), 96 * 250_000)
        self.assertEqual(sum(b.intervals["export"].values()), 20 * 500_000)
        self.assertTrue(all(v >= 0 for v in b.intervals["export"].values()))
        self.assertEqual(sum(b.intervals["net"].values()), 96 * 250_000 - 20 * 500_000)

    def test_configured_meter_and_missing_configured_meter(self):
        b = P.build_batch(payload(day_points(2026, 11, 10)), DEN, preferred_meter="M2")
        self.assertEqual((b.meter, b.mode), ("M2", P.MODE_FORWARD_ONLY))
        with self.assertRaises(P.IncompleteDataError):
            P.build_batch(payload(day_points(2026, 11, 10)), DEN, preferred_meter="NOPE")

    def test_net_only_meter_splits_by_sign(self):
        meters = [{"meterNumber": "N", "channel": 1, "flowDirection": "NET", "seriesId": "N - Net", "isNetMeter": True}]
        pl = {"data": {"ELECTRIC": [{"type": "USAGE", "meters": meters, "series": [
            {"name": "N - Net", "data": [{"x": wall_ms(2026, 11, 10, 0, 0), "y": 0.4}, {"x": wall_ms(2026, 11, 10, 0, 15), "y": -0.3}]}]}]}}
        b = P.build_batch(pl, DEN)
        self.assertEqual(b.mode, P.MODE_NET_SPLIT)
        self.assertEqual(sum(b.intervals["import"].values()), 400_000)
        self.assertEqual(sum(b.intervals["export"].values()), 300_000)
        self.assertEqual(sum(b.intervals["net"].values()), 100_000)

    def test_unknown_direction_is_ignored_not_fatal(self):
        pts = day_points(2026, 11, 10)
        pl = payload(pts)
        pl["data"]["ELECTRIC"][0]["meters"].append(
            {"meterNumber": "M1", "channel": 9, "flowDirection": "WEIRD", "seriesId": "M1 - Weird", "isNetMeter": True})
        b = P.build_batch(pl, DEN)
        self.assertEqual(b.unknown_channels, ["M1/9:WEIRD"])

    def test_declared_series_missing_is_incomplete(self):
        pl = payload(day_points(2026, 11, 10))
        entry = pl["data"]["ELECTRIC"][0]
        entry["series"] = [s for s in entry["series"] if s["name"] != "M1 - Generation"]
        with self.assertRaises(P.IncompleteDataError):
            P.build_batch(pl, DEN)

    def test_empty_and_malformed_payloads(self):
        for bad in (None, {}, {"data": {}}, {"data": {"ELECTRIC": []}}, {"data": {"ELECTRIC": [{"type": "COST"}]}}):
            with self.assertRaises(P.IncompleteDataError):
                P.build_batch(bad, DEN)
        pl = payload(day_points(2026, 11, 10))
        for s in pl["data"]["ELECTRIC"][0]["series"]:
            s["data"] = []
        with self.assertRaises(P.IncompleteDataError):
            P.build_batch(pl, DEN)

    def test_invalid_values_rejected_and_negative_import_clamped(self):
        pts = day_points(2026, 11, 10, hours=1)
        pl = payload(pts)
        cons = pl["data"]["ELECTRIC"][0]["series"][1]["data"]
        cons[0]["y"] = None
        cons[1]["y"] = float("nan")
        cons[2]["y"] = -0.2
        b = P.build_batch(pl, DEN)
        self.assertEqual(b.invalid_points, 2)
        self.assertEqual(b.clamped_points, 1)
        self.assertTrue(all(v >= 0 for v in b.intervals["import"].values()))

    def test_common_end_truncation(self):
        pts = day_points(2026, 11, 10, hours=3)
        pl = payload(pts)
        gen = pl["data"]["ELECTRIC"][0]["series"][2]["data"]
        del gen[-4:]  # generation lags by one hour
        b = P.build_batch(pl, DEN)
        self.assertEqual(max(b.intervals["import"]), max(b.intervals["export"]))
        self.assertEqual(b.data_end.astimezone(DEN), datetime(2026, 11, 10, 2, 0, tzinfo=DEN))


class TimeHandling(unittest.TestCase):
    def test_wall_clock_relabel(self):
        t = P.relabel_wall_clock(wall_ms(2026, 11, 16, 2, 15), DEN)
        self.assertEqual(t.astimezone(UTC), datetime(2026, 11, 16, 8, 15, tzinfo=UTC))
        t = P.relabel_wall_clock(wall_ms(2026, 7, 1, 12), DEN)
        self.assertEqual(t.astimezone(UTC), datetime(2026, 7, 1, 17, tzinfo=UTC))

    def test_noon_export_lands_at_local_noon(self):
        b = P.build_batch(payload(day_points(2026, 11, 10)), DEN)
        hours = P.hourly(b.intervals["export"])
        local_hours = sorted({h.astimezone(DEN).hour for h, v in hours.items() if v})
        self.assertEqual(local_hours, [10, 11, 12, 13, 14])

    def test_fall_back_day_no_duplicate_hours_and_marker_point(self):
        pts = []
        for h in range(5):
            for q in range(4):
                pts.append((wall_ms(2026, 11, 1, h, q * 15), 0.1, 0.0, 0.0))
        pts.insert(8, (wall_ms(2026, 11, 1, 1, 57), 0.0, 0.0, 0.0))  # DST marker
        pts.sort()
        b = P.build_batch(payload(pts), DEN)
        hours = P.hourly(b.intervals["import"])
        self.assertEqual(len(hours), 5)
        self.assertEqual(sum(hours.values()), 20 * 100_000)
        self.assertEqual(len(set(hours)), len(hours))
        # wall 01:00 -> first occurrence (daylight time); the repeated standard-time 01:00 hour has no data
        self.assertIn(datetime(2026, 11, 1, 6, tzinfo=UTC), hours)
        self.assertNotIn(datetime(2026, 11, 1, 7, tzinfo=UTC), hours)
        rows = P.contiguous_rows(hours, min(hours), max(hours))
        self.assertEqual([r[0] for r in rows], [min(hours) + timedelta(hours=i) for i in range(len(rows))])
        self.assertEqual(rows[2], (datetime(2026, 11, 1, 7, tzinfo=UTC), 0))

    def test_spring_forward_day(self):
        pts = []
        for h in (0, 1, 3, 4):
            for q in range(4):
                pts.append((wall_ms(2026, 3, 8, h, q * 15), 0.1, 0.0, 0.0))
        pts.append((wall_ms(2026, 3, 8, 1, 57), 0.0, 0.0, 0.0))
        pts.sort()
        b = P.build_batch(payload(pts), DEN)
        hours = P.hourly(b.intervals["import"])
        self.assertEqual(sorted(hours), [datetime(2026, 3, 8, 6, tzinfo=UTC), datetime(2026, 3, 8, 7, tzinfo=UTC),
                                         datetime(2026, 3, 8, 8, tzinfo=UTC), datetime(2026, 3, 8, 9, tzinfo=UTC)])
        self.assertEqual(sum(hours.values()), 16 * 100_000)

    def test_interval_length_and_data_end(self):
        b = P.build_batch(payload(day_points(2026, 11, 10, hours=3)), DEN)
        self.assertEqual(b.interval_length, timedelta(minutes=15))
        self.assertEqual(b.data_end.astimezone(DEN), datetime(2026, 11, 10, 3, tzinfo=DEN))

    def test_window_filter_and_merge_dedupes_overlap(self):
        pts = day_points(2026, 11, 10) + day_points(2026, 11, 11)
        full = P.build_batch(payload(pts), DEN)
        mid = datetime(2026, 11, 11, tzinfo=DEN)
        a = P.build_batch(payload(pts), DEN, window=(datetime(2026, 11, 10, tzinfo=DEN), mid))
        # overlapping second chunk (API may return an extra leading hour)
        b = P.build_batch(payload(pts), DEN, window=(mid, datetime(2026, 11, 12, tzinfo=DEN)))
        merged = a.merge(b)
        for role in ("import", "export", "net"):
            self.assertEqual(merged.intervals[role], full.intervals[role])
        self.assertEqual(merged.data_end, full.data_end)


class ChainsAndMonth(unittest.TestCase):
    def test_chain_is_contiguous_monotonic_and_exact(self):
        hours = {datetime(2026, 11, 10, h, tzinfo=UTC): 140_000 for h in (0, 1, 3)}
        rows = P.contiguous_rows(hours, datetime(2026, 11, 10, 0, tzinfo=UTC), datetime(2026, 11, 10, 3, tzinfo=UTC))
        self.assertEqual(len(rows), 4)
        chained = P.chain(rows, base_micro=13_799_790_000)
        sums = [s for _t, _v, s in chained]
        self.assertEqual(sums, sorted(sums))
        self.assertEqual(sums[-1], 13_799_790_000 + 3 * 140_000)
        self.assertEqual(chained[2][1], 0)

    def test_month_to_date_uses_newest_reading_month(self):
        pts = day_points(2026, 10, 31) + day_points(2026, 11, 1, hours=3)
        b = P.build_batch(payload(pts), DEN)
        m = P.month_to_date(b, DEN, fetched_from=datetime(2026, 10, 1, tzinfo=DEN))
        self.assertEqual(m["month_start"], datetime(2026, 11, 1, tzinfo=DEN))
        self.assertEqual(m["month_start"].utcoffset(), timedelta(hours=-5))  # Nov 1 00:00 is still daylight time
        self.assertAlmostEqual(m["import"], 12 * 0.25)
        # Before the 1st's batch arrives the sensor still reports October
        b2 = P.build_batch(payload(day_points(2026, 10, 31)), DEN)
        m2 = P.month_to_date(b2, DEN, fetched_from=datetime(2026, 10, 1, tzinfo=DEN))
        self.assertEqual(m2["month_start"], datetime(2026, 10, 1, tzinfo=DEN))

    def test_month_to_date_can_be_negative_and_requires_coverage(self):
        pts = [(wall_ms(2026, 6, 2, h, q * 15), 0.1, -0.6, 0) for h in range(24) for q in range(4)]
        b = P.build_batch(payload(pts), DEN)
        m = P.month_to_date(b, DEN, fetched_from=datetime(2026, 6, 1, tzinfo=DEN))
        self.assertLess(m["net"], 0)
        with self.assertRaises(P.IncompleteDataError):
            P.month_to_date(b, DEN, fetched_from=datetime(2026, 6, 2, tzinfo=DEN))


class Scheduling(unittest.TestCase):
    def setUp(self):
        # 3 observed daily batches: data through 02:30 local, seen ~05:40
        self.arrivals = [
            (datetime(2026, 11, d, 12, 40, tzinfo=UTC), datetime(2026, 11, d, 9, 30, tzinfo=UTC)) for d in (13, 14, 15)
        ]
        self.data_end = datetime(2026, 11, 16, 8, 30, tzinfo=UTC)

    def test_learning_without_history(self):
        s = P.compute_schedule(datetime(2026, 11, 16, 16, tzinfo=UTC), self.data_end, [])
        self.assertEqual((s.wait, s.mode), (timedelta(hours=1), "learning"))
        s = P.compute_schedule(datetime(2026, 11, 16, 16, tzinfo=UTC), self.data_end, [], fixed_interval=timedelta(hours=6))
        self.assertEqual(s.wait, timedelta(hours=6))

    def test_idle_until_window_then_15_min_then_backoff(self):
        expected = self.data_end + timedelta(days=1) + timedelta(hours=3, minutes=10)
        s = P.compute_schedule(datetime(2026, 11, 16, 16, tzinfo=UTC), self.data_end, self.arrivals)
        self.assertEqual(s.mode, "idle_until_window")
        self.assertEqual(s.expected_arrival, expected)
        self.assertLessEqual(s.wait, P.MAX_IDLE)
        s = P.compute_schedule(expected - timedelta(minutes=30), self.data_end, self.arrivals)
        self.assertEqual((s.mode, s.wait), ("window", timedelta(minutes=15)))
        s = P.compute_schedule(expected + timedelta(hours=3), self.data_end, self.arrivals)
        self.assertEqual((s.mode, s.wait), ("overdue_backoff", timedelta(minutes=30)))
        s = P.compute_schedule(expected + timedelta(hours=12), self.data_end, self.arrivals)
        self.assertEqual(s.wait, timedelta(hours=2))

    def test_idle_capped_at_max_idle(self):
        s = P.compute_schedule(datetime(2026, 11, 16, 8, 45, tzinfo=UTC), self.data_end, self.arrivals)
        self.assertEqual(s.wait, P.MAX_IDLE)

    def test_failure_backoff_overrides(self):
        waits = [P.compute_schedule(datetime(2026, 11, 15, 23, tzinfo=UTC), self.data_end, self.arrivals, n).wait
                 for n in (1, 2, 3, 4, 5, 9)]
        self.assertEqual([w.total_seconds() / 60 for w in waits], [15, 30, 60, 120, 360, 360])

    def test_frequent_publisher_uses_fixed_rate(self):
        arrivals = [(datetime(2026, 11, 16, h, 20, tzinfo=UTC), datetime(2026, 11, 16, h, tzinfo=UTC)) for h in range(5)]
        s = P.compute_schedule(datetime(2026, 11, 16, 5, tzinfo=UTC), arrivals[-1][1], arrivals)
        self.assertEqual(s.mode, "frequent_publish")


class CoverageAndGuards(unittest.TestCase):
    def test_complete_hours_and_missing_interval(self):
        pts = day_points(2026, 11, 10, hours=3)
        pl = payload(pts)
        gen = pl["data"]["ELECTRIC"][0]["series"][2]["data"]
        del gen[5]  # export missing 01:15 local
        b = P.build_batch(pl, DEN)
        h0 = datetime(2026, 11, 10, 6, tzinfo=UTC)
        h1 = datetime(2026, 11, 10, 7, tzinfo=UTC)
        self.assertTrue(b.hour_complete("import", h0) and b.hour_complete("export", h0))
        self.assertTrue(b.hour_complete("import", h1))
        self.assertFalse(b.hour_complete("export", h1))
        self.assertFalse(b.hour_complete("net", h1))

    def test_explicit_zero_counts_as_present(self):
        pts = [(wall_ms(2026, 11, 10, 0, q * 15), 0.0, 0.0, 0.0) for q in range(4)]
        b = P.build_batch(payload(pts), DEN)
        self.assertTrue(b.hour_complete("import", datetime(2026, 11, 10, 6, tzinfo=UTC)))

    def test_partial_last_hour_is_incomplete(self):
        pts = day_points(2026, 11, 16, hours=3)[:-2]  # ends 02:15 -> 02:00 hour has 2 slots
        b = P.build_batch(payload(pts), DEN)
        last = datetime(2026, 11, 16, 8, tzinfo=UTC)
        self.assertEqual(len(b.coverage["import"][last]), 2)
        self.assertFalse(b.hour_complete("import", last))

    def test_conflicting_duplicates_mark_hour_incomplete(self):
        pts = day_points(2026, 11, 10, hours=1)
        pl = payload(pts)
        cons = pl["data"]["ELECTRIC"][0]["series"][1]["data"]
        cons.append({"x": cons[0]["x"], "y": 0.9})   # conflicting duplicate
        cons.append({"x": cons[1]["x"], "y": cons[1]["y"]})  # identical duplicate: deduped
        b = P.build_batch(pl, DEN)
        h = datetime(2026, 11, 10, 6, tzinfo=UTC)
        self.assertFalse(b.hour_complete("import", h))
        self.assertEqual(sum(b.intervals["import"].values()), 900_000 + 3 * 250_000)

    def test_lowering_guard(self):
        pts = day_points(2026, 11, 10, hours=2)
        pl = payload(pts)
        cons = pl["data"]["ELECTRIC"][0]["series"][1]["data"]
        del cons[6]  # 01:30 missing -> hour 08Z incomplete for import
        b = P.build_batch(pl, DEN)
        rows = P.contiguous_rows(P.hourly(b.intervals["import"]), datetime(2026, 11, 10, 6, tzinfo=UTC),
                                 datetime(2026, 11, 10, 7, tzinfo=UTC))
        stored = {datetime(2026, 11, 10, 6, tzinfo=UTC): 1_200_000, datetime(2026, 11, 10, 7, tzinfo=UTC): 1_000_000}
        # hour 07Z complete -> lowering 1.2 -> 1.0 allowed; hour 08Z incomplete 0.75 < 1.0 -> blocked
        self.assertEqual(P.lowering_incomplete_hours(b, "import", rows, stored), [datetime(2026, 11, 10, 7, tzinfo=UTC)])
        stored[datetime(2026, 11, 10, 7, tzinfo=UTC)] = 750_000
        self.assertEqual(P.lowering_incomplete_hours(b, "import", rows, stored), [])

    def test_merge_detects_conflicting_overlap(self):
        pts = day_points(2026, 11, 10)
        a = P.build_batch(payload(pts), DEN, truncate=False)
        pts2 = [(x, (0.3 if i == 0 else c), g, o) for i, (x, c, g, o) in enumerate(pts)]
        b = P.build_batch(payload(pts2), DEN, truncate=False)
        a.merge(b)
        self.assertFalse(a.hour_complete("import", datetime(2026, 11, 10, 6, tzinfo=UTC)))


class SchedulingMissedBatch(unittest.TestCase):
    def test_missed_batch_does_not_inflate_step(self):
        ends = [datetime(2026, 11, d, 9, 30, tzinfo=UTC) for d in (12, 14)]  # day 13 missed
        arrivals = [(e + timedelta(hours=3), e) for e in ends] + [(datetime(2026, 11, 15, 12, 30, tzinfo=UTC), datetime(2026, 11, 15, 9, 30, tzinfo=UTC))]
        s = P.compute_schedule(datetime(2026, 11, 15, 18, tzinfo=UTC), datetime(2026, 11, 15, 9, 30, tzinfo=UTC), arrivals)
        self.assertEqual(s.expected_arrival, datetime(2026, 11, 16, 12, 30, tzinfo=UTC))

    def test_explicit_interval_wins(self):
        s = P.compute_schedule(datetime(2026, 11, 14, 23, tzinfo=UTC), None, [], 0, timedelta(minutes=30))
        self.assertEqual((s.mode, s.wait), ("fixed", timedelta(minutes=30)))


class GapDetection(unittest.TestCase):
    def test_terminal_partial_and_dst_fold_exempt(self):
        pts = []
        for h in range(4):
            for q in range(4):
                pts.append((wall_ms(2026, 11, 1, h, q * 15), 0.1, 0.0, 0.0))
        pts = pts[:-2]  # terminal 03:00 hour partial
        b = P.build_batch(payload(pts), DEN)
        first = datetime(2026, 11, 1, 5, tzinfo=UTC)   # 00:00 daylight time
        last = P.floor_hour(b.data_end - b.interval_length)
        # the repeated 01:00 hour (fold=1, collapsed by SmartHub) has no data but is exempt
        self.assertEqual(P.unexpected_gaps(b, first, last, DEN), [])

    def test_interior_hole_is_a_gap(self):
        pts = day_points(2026, 11, 10, hours=4)
        del pts[6]
        b = P.build_batch(payload(pts), DEN)
        gaps = P.unexpected_gaps(b, datetime(2026, 11, 10, 6, tzinfo=UTC), P.floor_hour(b.data_end - b.interval_length), DEN)
        self.assertEqual(gaps, [datetime(2026, 11, 10, 7, tzinfo=UTC)])

    def test_missing_whole_hour_is_a_gap(self):
        pts = [p for p in day_points(2026, 11, 10, hours=4) if not (wall_ms(2026, 11, 10, 1) <= p[0] < wall_ms(2026, 11, 10, 2))]
        b = P.build_batch(payload(pts), DEN)
        gaps = P.unexpected_gaps(b, datetime(2026, 11, 10, 6, tzinfo=UTC), P.floor_hour(b.data_end - b.interval_length), DEN)
        self.assertEqual(gaps, [datetime(2026, 11, 10, 7, tzinfo=UTC)])


class RetryAfter(unittest.TestCase):
    def test_parse(self):
        import importlib.util, types
        # api.py imports HA-free modules only through relative imports; load the helper source directly
        with open(os.path.join(os.path.abspath(COMPONENT), "api.py")) as fh:
            src = fh.read()
        start = src.index("def _retry_after_seconds")
        end = src.index("\n\n\nclass ParseType")
        ns = {"Optional": __import__("typing").Optional, "datetime": datetime, "timezone": timezone}
        exec(src[start:end], ns)
        f = ns["_retry_after_seconds"]
        self.assertEqual(f("120"), 120.0)
        self.assertIsNone(f(None))
        self.assertIsNone(f("soon"))
        future = (datetime.now(timezone.utc) + timedelta(seconds=90)).strftime("%a, %d %b %Y %H:%M:%S GMT")
        self.assertTrue(80 <= f(future) <= 91)


class ResolveHours(unittest.TestCase):
    def setUp(self):
        pts = day_points(2026, 11, 10, hours=3, gen_at_noon=-0.5)
        pts = [(x, c, (-0.5 if i < 8 else 0.0), o) for i, (x, c, g, o) in enumerate(pts)]  # export in first 2 hours
        self.pl = payload(pts)
        self.h0 = datetime(2026, 11, 10, 6, tzinfo=UTC)
        self.h1 = datetime(2026, 11, 10, 7, tzinfo=UTC)
        self.h2 = datetime(2026, 11, 10, 8, tzinfo=UTC)

    def test_incomplete_export_keeps_fuller_stored_and_net_consistent(self):
        gen = self.pl["data"]["ELECTRIC"][0]["series"][2]["data"]
        del gen[5]  # export 01:15 missing -> hour h1 incomplete, export 1.5 instead of 2.0
        b = P.build_batch(self.pl, DEN)
        stored = {"import": {self.h1: 1_000_000}, "export": {self.h1: 2_000_000}, "net": {self.h1: -1_000_000}}
        rows, kept = P.resolve_hours(b, self.h0, self.h2, stored)
        r = {role: dict(v) for role, v in rows.items()}
        self.assertEqual(kept, [self.h1])
        self.assertEqual((r["import"][self.h1], r["export"][self.h1], r["net"][self.h1]), (1_000_000, 2_000_000, -1_000_000))
        for h in (self.h0, self.h1, self.h2):
            self.assertEqual(r["net"][h], r["import"][h] - r["export"][h])

    def test_complete_downward_revision_and_terminal_growth_use_new(self):
        b = P.build_batch(self.pl, DEN)
        stored = {"import": {self.h0: 5_000_000, self.h2: 100_000}, "export": {self.h0: 2_000_000, self.h2: 0},
                  "net": {self.h0: 3_000_000, self.h2: 100_000}}
        rows, kept = P.resolve_hours(b, self.h0, self.h2, stored)
        r = {role: dict(v) for role, v in rows.items()}
        self.assertEqual(kept, [])
        self.assertEqual(r["import"][self.h0], 1_000_000)   # complete hour revised down: trusted
        self.assertEqual(r["import"][self.h2], 1_000_000)   # terminal hour grew
        self.assertEqual(P.totals_since(rows, self.h1)["import"], 2_000_000)

    def test_no_stored_values_never_invents(self):
        gen = self.pl["data"]["ELECTRIC"][0]["series"][2]["data"]
        del gen[5]
        b = P.build_batch(self.pl, DEN)
        rows, kept = P.resolve_hours(b, self.h0, self.h2, {})
        self.assertEqual(kept, [])
        self.assertEqual(dict(rows["export"])[self.h1], 1_500_000)

    def test_simultaneous_revision_with_incomplete_export_keeps_pair(self):
        gen = self.pl["data"]["ELECTRIC"][0]["series"][2]["data"]
        del gen[5]  # h1 export incomplete -> 1.5
        cons = self.pl["data"]["ELECTRIC"][0]["series"][1]["data"]
        for i in range(4, 8):
            cons[i]["y"] = 0.5  # h1 import 2.0 (up from stored 1.0)
        b = P.build_batch(self.pl, DEN)
        stored = {"import": {self.h1: 1_000_000}, "export": {self.h1: 2_000_000}, "net": {self.h1: -1_000_000}}
        rows, kept = P.resolve_hours(b, self.h0, self.h2, stored)
        r = {role: dict(v) for role, v in rows.items()}
        self.assertEqual(kept, [self.h1])
        self.assertEqual((r["import"][self.h1], r["export"][self.h1], r["net"][self.h1]), (1_000_000, 2_000_000, -1_000_000))
        self.assertEqual(P.lowering_incomplete_hours(b, "export", rows["export"], stored["export"]), [])

    def test_no_readings_only_when_all_series_wellformed_and_empty(self):
        win = (datetime(2026, 11, 10, tzinfo=DEN), datetime(2026, 11, 11, tzinfo=DEN))
        pl = payload(day_points(2026, 11, 10, hours=2))
        pl["data"]["ELECTRIC"][0]["series"][1]["data"] = []  # import empty, export populated
        with self.assertRaises(P.IncompleteDataError) as ctx:
            P.build_batch(pl, DEN, window=win)
        self.assertNotIsInstance(ctx.exception, P.NoReadingsError)
        pl = payload(day_points(2026, 11, 10, hours=2))
        del pl["data"]["ELECTRIC"][0]["series"][1]["data"]  # missing data field
        pl["data"]["ELECTRIC"][0]["series"][2]["data"] = []
        with self.assertRaises(P.IncompleteDataError) as ctx:
            P.build_batch(pl, DEN, window=win)
        self.assertNotIsInstance(ctx.exception, P.NoReadingsError)
        pl = payload(day_points(2026, 11, 10, hours=2))
        for s_ in pl["data"]["ELECTRIC"][0]["series"]:
            s_["data"] = [{"x": p_["x"], "y": None} for p_ in s_["data"]]  # all invalid
        with self.assertRaises(P.IncompleteDataError) as ctx:
            P.build_batch(pl, DEN, window=win)
        self.assertNotIsInstance(ctx.exception, P.NoReadingsError)

    def test_empty_series_raises_no_readings(self):
        pl = payload(day_points(2026, 11, 10, hours=1))
        with self.assertRaises(P.NoReadingsError):
            P.build_batch(pl, DEN, window=(datetime(2026, 12, 1, tzinfo=DEN), datetime(2026, 12, 2, tzinfo=DEN)))


if __name__ == "__main__":
    unittest.main()

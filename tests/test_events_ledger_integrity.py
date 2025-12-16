import csv
from datetime import datetime
from zoneinfo import ZoneInfo

import mm.market_data.recorder as recorder_mod


class DummyLob:
    def __init__(self, last_update_id=10):
        self.last_update_id = last_update_id
        self.bids = {100.0: 1.0}
        self.asks = {101.0: 1.0}

    def apply_diff(self, U, u, bids, asks):
        self.last_update_id = int(u)
        return True

    def top_n(self, n):
        bids = sorted(self.bids.items(), reverse=True)[:n]
        asks = sorted(self.asks.items())[:n]
        return bids, asks


def test_events_contains_run_start_snapshot_synced(monkeypatch, tmp_path):
    # Keep within recording window
    fixed_now = datetime(2025, 12, 15, 12, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    monkeypatch.setattr(recorder_mod, "berlin_now", lambda: fixed_now)

    monkeypatch.setenv("SYMBOL", "ETHUSDT")

    # Redirect data/
    orig_path = recorder_mod.Path

    def PatchedPath(p):
        if p == "data":
            return orig_path(tmp_path / "data")
        return orig_path(p)

    monkeypatch.setattr(recorder_mod, "Path", PatchedPath)

    # Avoid real logs
    monkeypatch.setattr(recorder_mod, "setup_logging", lambda *args, **kwargs: tmp_path / "log.txt")

    # Fake snapshot writer
    def fake_record_rest_snapshot(client, symbol, day_dir, snapshots_dir, limit, run_id, event_id, tag, decimals=8):
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        snap_path = snapshots_dir / f"snapshot_{event_id:06d}_{tag}.csv"
        snap_path.write_text("dummy\n", encoding="utf-8")
        return DummyLob(last_update_id=10), snap_path, 10

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", fake_record_rest_snapshot)

    # Fake WS: sync + one applied update
    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, insecure_tls):
            self.on_open = on_open
            self.on_depth = on_depth

        def run_forever(self):
            self.on_open()
            # bridging (sync)
            self.on_depth({"e": "depthUpdate", "E": 1, "U": 10, "u": 11, "b": [], "a": []}, 111)
            # applied
            self.on_depth({"e": "depthUpdate", "E": 2, "U": 12, "u": 12, "b": [], "a": []}, 222)

        def close(self):
            pass

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    # Deterministic run_id
    monkeypatch.setattr(recorder_mod.time, "time", lambda: 1.0)

    recorder_mod.run_recorder()

    date = recorder_mod.datetime.utcnow().strftime("%Y%m%d")
    day_dir = tmp_path / "data" / "ETHUSDT" / date

    events_path = day_dir / "events.csv"
    orderbook_path = day_dir / "orderbook.csv"

    assert events_path.exists()
    assert orderbook_path.exists()

    # Parse events
    rows = list(csv.reader(events_path.open()))
    header = rows[0]
    assert header == ["event_id", "ts_recv_ms", "run_id", "type", "epoch_id", "details"]

    types = [r[3] for r in rows[1:]]
    assert "run_start" in types
    assert "snapshot_taken" in types
    assert "synced" in types

    # Ensure we wrote at least one orderbook row and it is in a valid epoch (>=1)
    ob_rows = list(csv.reader(orderbook_path.open()))
    assert ob_rows[0][0:2] == ["run_id", "epoch_id"]
    assert len(ob_rows) >= 2
    first_data_epoch = int(ob_rows[1][1])
    assert first_data_epoch >= 1

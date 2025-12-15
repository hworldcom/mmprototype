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
        # Always succeed and advance update id
        self.last_update_id = u
        return True

    def top_n(self, n):
        bids = sorted(self.bids.items(), reverse=True)[:n]
        asks = sorted(self.asks.items())[:n]
        return bids, asks


def test_appends_and_run_id_changes(monkeypatch, tmp_path):
    # Ensure we are inside the recording window
    fixed_now = datetime(2025, 12, 15, 12, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    monkeypatch.setattr(recorder_mod, "berlin_now", lambda: fixed_now)

    monkeypatch.setenv("SYMBOL", "ETHUSDT")

    # Redirect data/ to tmp_path/data/
    original_path = recorder_mod.Path

    def PatchedPath(p):
        if p == "data":
            return original_path(tmp_path / "data")
        return original_path(p)

    monkeypatch.setattr(recorder_mod, "Path", PatchedPath)

    # Avoid touching real logs
    monkeypatch.setattr(recorder_mod, "setup_logging", lambda *args, **kwargs: tmp_path / "log.txt")

    # Patch snapshot to return deterministic dummy lob and create a snapshot file
    def fake_record_rest_snapshot(client, symbol, out_dir, limit=1000, tag="initial"):
        date = recorder_mod.datetime.utcnow().strftime("%Y%m%d")
        p = out_dir / f"orderbook_rest_snapshot_{symbol}_{date}_{tag}.csv"
        p.write_text("side,price,qty\n", encoding="utf-8")
        return DummyLob(last_update_id=10), p

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", fake_record_rest_snapshot)

    # Fake WS stream: open, then emit one bridging depth event, then one applied depth event, then stop
    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, insecure_tls):
            self.on_depth = on_depth
            self.on_open = on_open

        def run_forever(self):
            self.on_open()
            # bridging event (will sync engine)
            self.on_depth({"E": 1, "U": 10, "u": 11, "b": [], "a": []}, 111)
            # another event (applied)
            self.on_depth({"E": 2, "U": 12, "u": 12, "b": [], "a": []}, 222)

        def close(self):
            pass

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    # Run 1: time.time fixed to produce run_id=1000ms
    monkeypatch.setattr(recorder_mod.time, "time", lambda: 1.0)
    recorder_mod.run_recorder()

    # Run 2: time.time fixed to produce run_id=2000ms
    monkeypatch.setattr(recorder_mod.time, "time", lambda: 2.0)
    recorder_mod.run_recorder()

    # Assert: orderbook file exists and has header + 2+ rows (we write 2 depth rows per run; synced+applied)
    symbol_dir = tmp_path / "data" / "ETHUSDT"
    ob_files = list(symbol_dir.glob("orderbook_ws_depth_ETHUSDT_*.csv"))
    assert len(ob_files) == 1

    rows = list(csv.reader(ob_files[0].open()))
    header = rows[0]
    assert header[0] == "run_id"

    data_rows = rows[1:]
    assert len(data_rows) >= 2  # at least one row per run

    # First row run_id should differ from last row run_id across two runs
    first_run_id = data_rows[0][0]
    last_run_id = data_rows[-1][0]
    assert first_run_id != last_run_id

# tests/test_recorder_resync_files.py

import csv
import mm.market_data.recorder as recorder_mod


class DummyLob:
    def __init__(self, last_update_id):
        self.last_update_id = last_update_id
        self.bids = {100.0: 1.0}
        self.asks = {101.0: 1.0}
        self._calls = 0

    def apply_diff(self, U, u, bids, asks):
        self._calls += 1
        if self._calls == 1:
            self.last_update_id = u
            return True
        return False

    def top_n(self, n):
        bids = sorted(self.bids.items(), reverse=True)[:n]
        asks = sorted(self.asks.items())[:n]
        return bids, asks


def test_resync_writes_gap_and_snapshot_tags(monkeypatch, tmp_path):
    monkeypatch.setenv("SYMBOL", "BTCUSDT")

    original_path = recorder_mod.Path

    def PatchedPath(p):
        if p == "data":
            return original_path(tmp_path / "data")
        return original_path(p)

    monkeypatch.setattr(recorder_mod, "Path", PatchedPath)
    monkeypatch.setattr(recorder_mod, "setup_logging", lambda *args, **kwargs: tmp_path / "log.txt")

    calls = {"tags": []}

    def fake_record_rest_snapshot(client, symbol, out_dir, limit=1000, tag="initial"):
        calls["tags"].append(tag)
        date = recorder_mod.datetime.utcnow().strftime("%Y%m%d")
        p = out_dir / f"orderbook_rest_snapshot_{symbol}_{date}_{tag}.csv"
        p.write_text("side,price,qty\n", encoding="utf-8")
        return DummyLob(last_update_id=10), p

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", fake_record_rest_snapshot)

    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, insecure_tls):
            self.on_depth = on_depth
            self.on_open = on_open

        def run_forever(self):
            self.on_open()
            self.on_depth({"E": 1, "U": 10, "u": 11, "b": [], "a": []}, 111)
            self.on_depth({"E": 2, "U": 12, "u": 12, "b": [], "a": []}, 222)

        def close(self):
            pass

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    recorder_mod.run_recorder()

    assert "initial" in calls["tags"]
    assert any(t.startswith("resync_") for t in calls["tags"])

    symbol_dir = tmp_path / "data" / "BTCUSDT"
    gaps = list(symbol_dir.glob("gaps_BTCUSDT_*.csv"))
    assert len(gaps) == 1

    rows = list(csv.reader(gaps[0].open()))
    assert rows[0] == ["recv_time_ms", "event", "details"]
    events = [r[1] for r in rows[1:]]
    assert "resync_start" in events
    assert "resync_done" in events

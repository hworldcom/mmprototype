from __future__ import annotations

import gzip
import json
import csv
from pathlib import Path

from mm.backtest.io import DepthDiff, Trade, iter_depth_diffs, iter_trades_csv


def test_depthdiff_fields_and_iter_depth_diffs_parses(tmp_path: Path):
    """Lock the DepthDiff schema to what replay expects."""
    dd = DepthDiff(
        recv_ms=1000,
        E=900,
        U=10,
        u=12,
        b=[[100.0, 1.0]],
        a=[[101.0, 2.0]],
        recv_seq=55,
    )
    assert dd.recv_ms == 1000
    assert dd.E == 900
    assert dd.U == 10 and dd.u == 12
    assert dd.b[0][0] == 100.0 and dd.a[0][0] == 101.0
    assert dd.recv_seq == 55

    p = tmp_path / "depth_diffs.ndjson.gz"
    rows = [
        {"recv_ms": 2000, "recv_seq": 99, "E": 1500, "U": 1, "u": 1, "b": [[1, 2]], "a": [[3, 4]]},
        {"recv_ms": 2100,             "E": 1600, "U": 2, "u": 3, "b": [], "a": []},  # missing recv_seq => None
    ]
    with gzip.open(p, "wt", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")

    out = list(iter_depth_diffs(p))
    assert len(out) == 2
    assert out[0].recv_ms == 2000 and out[0].recv_seq == 99
    assert out[1].recv_ms == 2100 and out[1].recv_seq is None


def test_trade_fields_and_iter_trades_csv_parses(tmp_path: Path):
    """Lock the Trade schema to what replay/backtest expects."""
    tr = Trade(
        recv_ms=1000,
        E=900,
        price=100.0,
        qty=0.1,
        is_buyer_maker=1,
        trade_id=123,
        trade_time_ms=901,
        recv_seq=42,
    )
    assert tr.recv_ms == 1000
    assert tr.E == 900
    assert tr.price == 100.0 and tr.qty == 0.1
    assert tr.is_buyer_maker == 1
    assert tr.trade_id == 123
    assert tr.trade_time_ms == 901
    assert tr.recv_seq == 42

    p = tmp_path / "trades.csv"
    with p.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "event_time_ms",
                "recv_time_ms",
                "recv_seq",
                "trade_id",
                "trade_time_ms",
                "price",
                "qty",
                "is_buyer_maker",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "event_time_ms": "3000",
                "recv_time_ms": "3050",
                "recv_seq": "777",
                "trade_id": "10",
                "trade_time_ms": "3001",
                "price": "100.5",
                "qty": "0.25",
                "is_buyer_maker": "0",
            }
        )
        # Missing optional fields and recv_time_ms fallback
        w.writerow(
            {
                "event_time_ms": "3100",
                "recv_time_ms": "3100",
                "recv_seq": "",
                "trade_id": "",
                "trade_time_ms": "",
                "price": "100.6",
                "qty": "0.30",
                "is_buyer_maker": "1",
            }
        )


    # Also ensure fallback works when the recv_time_ms column is absent entirely.
    p2 = tmp_path / "trades_no_recv.csv"
    with p2.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "event_time_ms",
                "recv_seq",
                "trade_id",
                "trade_time_ms",
                "price",
                "qty",
                "is_buyer_maker",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "event_time_ms": "4000",
                "recv_seq": "888",
                "trade_id": "20",
                "trade_time_ms": "4001",
                "price": "101.0",
                "qty": "0.10",
                "is_buyer_maker": "1",
            }
        )
    out2 = list(iter_trades_csv(p2))
    assert out2[0].recv_ms == 4000

    out = list(iter_trades_csv(p))
    assert len(out) == 2
    assert out[0].E == 3000 and out[0].recv_ms == 3050 and out[0].recv_seq == 777
    assert out[1].E == 3100 and out[1].recv_ms == 3100 and out[1].recv_seq is None
    assert out[1].trade_id is None and out[1].trade_time_ms is None

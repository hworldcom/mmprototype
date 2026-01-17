from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from mm.backtest.io import find_trades_file, find_events_file, iter_trades_csv, iter_events_csv


def _write_gz_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        # write header
        f.write(",".join(header) + "\n")
        for r in rows:
            f.write(",".join(str(x) for x in r) + "\n")


def test_find_trades_and_events_prefers_gz(tmp_path: Path) -> None:
    root = tmp_path
    symbol = "BTCUSDT"
    yyyymmdd = "20251223"
    ddir = root / symbol / yyyymmdd
    # Create both .csv and .csv.gz; should prefer .csv.gz
    trades_gz = ddir / f"trades_ws_{symbol}_{yyyymmdd}.csv.gz"
    trades_csv = ddir / f"trades_ws_{symbol}_{yyyymmdd}.csv"
    events_gz = ddir / f"events_{symbol}_{yyyymmdd}.csv.gz"
    events_csv = ddir / f"events_{symbol}_{yyyymmdd}.csv"

    _write_gz_csv(
        trades_gz,
        header=["event_time_ms","recv_time_ms","price","qty","is_buyer_maker","trade_id","trade_time_ms","recv_seq"],
        rows=[[1,1,100.0,0.1,1,10,1,5]],
    )
    trades_csv.parent.mkdir(parents=True, exist_ok=True)
    trades_csv.write_text("event_time_ms,recv_time_ms,price,qty,is_buyer_maker\n1,1,100,0.1,1\n")

    _write_gz_csv(
        events_gz,
        header=["event_id","recv_time_ms","run_id","type","epoch_id","details_json","recv_seq"],
        rows=[[1,1,7,"TEST",0,"{}",9]],
    )
    events_csv.write_text("event_id,recv_time_ms,run_id,type,epoch_id,details_json\n1,1,7,TEST,0,{}\n")

    assert find_trades_file(root, symbol, yyyymmdd) == trades_gz
    assert find_events_file(root, symbol, yyyymmdd) == events_gz


def test_iter_trades_and_events_can_read_gz(tmp_path: Path) -> None:
    root = tmp_path
    symbol = "BTCUSDT"
    yyyymmdd = "20251223"
    ddir = root / symbol / yyyymmdd

    trades_gz = ddir / f"trades_ws_{symbol}_{yyyymmdd}.csv.gz"
    events_gz = ddir / f"events_{symbol}_{yyyymmdd}.csv.gz"

    _write_gz_csv(
        trades_gz,
        header=["event_time_ms","recv_time_ms","price","qty","is_buyer_maker","trade_id","trade_time_ms","recv_seq"],
        rows=[[1000,1001,100.0,0.25,0,123,999,42]],
    )
    _write_gz_csv(
        events_gz,
        header=["event_id","recv_time_ms","run_id","type","epoch_id","details_json","recv_seq"],
        rows=[[1,1001,2,"EPOCH",3,"{\"x\":1}",11]],
    )

    t = next(iter_trades_csv(trades_gz))
    assert t.E == 1000
    assert t.recv_ms == 1001
    assert t.price == 100.0
    assert t.qty == 0.25
    assert t.is_buyer_maker == 0
    assert t.trade_id == 123
    assert t.recv_seq == 42

    e = next(iter_events_csv(events_gz))
    assert e.event_id == 1
    assert e.recv_ms == 1001
    assert e.run_id == 2
    assert e.type == "EPOCH"
    assert e.epoch_id == 3
    assert e.recv_seq == 11

from __future__ import annotations

import csv
from pathlib import Path

from mm.backtest.io import EventRow, iter_events_csv


def test_eventrow_fields_and_iter_events_csv_parses(tmp_path: Path):
    """Lock the EventRow schema to what replay/backtest expects."""
    # 1) Dataclass must accept the canonical constructor args.
    er = EventRow(
        event_id=1,
        recv_ms=1000,
        run_id=7,
        type="snapshot_loaded",
        epoch_id=3,
        details_json="{}",
        recv_seq=42,
    )
    assert er.event_id == 1
    assert er.recv_ms == 1000
    assert er.run_id == 7
    assert er.type == "snapshot_loaded"
    assert er.epoch_id == 3
    assert er.details_json == "{}"
    assert er.recv_seq == 42

    # 2) iter_events_csv must parse a CSV with these columns (recv_seq optional).
    p = tmp_path / "events.csv"
    with p.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "event_id",
                "recv_time_ms",
                "recv_seq",
                "run_id",
                "type",
                "epoch_id",
                "details_json",
            ],
        )
        w.writeheader()
        w.writerow(
            {
                "event_id": "10",
                "recv_time_ms": "2000",
                "recv_seq": "99",
                "run_id": "8",
                "type": "resync_start",
                "epoch_id": "4",
                "details_json": "{\"reason\":\"gap\"}",
            }
        )
        w.writerow(
            {
                "event_id": "11",
                "recv_time_ms": "2100",
                "recv_seq": "",  # missing should map to None
                "run_id": "8",
                "type": "resync_end",
                "epoch_id": "4",
                "details_json": "{}",
            }
        )

    rows = list(iter_events_csv(p))
    assert len(rows) == 2
    assert rows[0].event_id == 10 and rows[0].recv_seq == 99
    assert rows[1].event_id == 11 and rows[1].recv_seq is None

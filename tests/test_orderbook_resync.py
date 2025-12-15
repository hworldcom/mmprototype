# tests/test_orderbook_resync.py

from mm.market_data.sync_engine import OrderBookSyncEngine
from mm.market_data.local_orderbook import LocalOrderBook


def test_gap_triggers_resync_needed():
    eng = OrderBookSyncEngine()

    # Build and adopt initial snapshot book
    lob = LocalOrderBook()
    lob.load_snapshot(
        bids=[["100", "1.0"]],
        asks=[["101", "1.0"]],
        last_update_id=10,
    )
    eng.adopt_snapshot(lob)

    # Feed a bridging event (should sync)
    r1 = eng.feed_depth_event({"U": 11, "u": 11, "b": [["100", "2.0"]], "a": [], "E": 1})
    assert r1.action in ("synced", "buffered")

    # Ensure synced: feed another sequential event if needed
    if not eng.depth_synced:
        r2 = eng.feed_depth_event({"U": 12, "u": 12, "b": [], "a": [["101", "0"]], "E": 2})
        assert eng.depth_synced or r2.action == "synced"

    assert eng.depth_synced is True
    assert eng.lob.last_update_id is not None

    # Now introduce a gap: U is too large
    last = eng.lob.last_update_id
    gap_U = last + 5
    gap_ev = {"U": gap_U, "u": gap_U, "b": [["99", "1.0"]], "a": [], "E": 3}

    r_gap = eng.feed_depth_event(gap_ev)
    assert r_gap.action == "gap"
    assert "gap" in r_gap.details  # details string includes "gap ..."

    # Reset and verify state cleared for resync
    eng.reset_for_resync()
    assert eng.snapshot_loaded is False
    assert eng.depth_synced is False
    assert eng.lob.last_update_id is None
    assert eng.buffer == []

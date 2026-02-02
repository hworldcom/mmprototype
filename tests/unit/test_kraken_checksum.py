from mm_core.checksum_engine import KrakenSyncEngine
from mm_recorder.exchanges.types import BookSnapshot, DepthDiff


def test_kraken_checksum_mismatch_triggers_gap():
    engine = KrakenSyncEngine(25)
    snap = BookSnapshot(
        event_time_ms=0,
        bids=[["100.0", "1.0"]],
        asks=[["101.0", "1.0"]],
        checksum=None,
    )
    engine.adopt_snapshot(snap)

    # Apply update with wrong checksum
    diff = DepthDiff(
        event_time_ms=0,
        U=0,
        u=0,
        bids=[["100.0", "1.0"]],
        asks=[["101.0", "1.0"]],
        checksum=123,
    )
    res = engine.feed_depth_event(diff)
    assert res.action == "gap"

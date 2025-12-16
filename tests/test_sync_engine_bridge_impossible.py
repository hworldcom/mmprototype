from mm.market_data.sync_engine import OrderBookSyncEngine
from mm.market_data.local_orderbook import LocalOrderBook


def test_bridge_impossible_triggers_gap():
    # Snapshot lastUpdateId = 100
    lob = LocalOrderBook()
    lob.load_snapshot(bids=[["100", "1.0"]], asks=[["101", "1.0"]], last_update_id=100)

    eng = OrderBookSyncEngine()
    eng.adopt_snapshot(lob)

    # Feed a diff whose earliest U is too far ahead: U > lastUpdateId + 1
    # This makes bridging impossible for this snapshot.
    r = eng.feed_depth_event({"E": 1, "U": 150, "u": 150, "b": [], "a": []})

    assert r.action == "gap"
    assert "bridge_impossible" in r.details

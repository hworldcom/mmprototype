from mm.market_data.sync_engine import OrderBookSyncEngine
from mm.market_data.local_orderbook import LocalOrderBook

def test_gap_triggers_gap_action():
    eng = OrderBookSyncEngine()
    lob = LocalOrderBook()
    lob.load_snapshot(bids=[["100", "1.0"]], asks=[["101", "1.0"]], last_update_id=10)
    eng.adopt_snapshot(lob)

    r1 = eng.feed_depth_event({"U": 10, "u": 11, "b": [], "a": [], "E": 1})
    if r1.action == "buffered":
        r1 = eng.feed_depth_event({"U": 10, "u": 11, "b": [], "a": [], "E": 2})
    assert eng.depth_synced

    last = eng.lob.last_update_id
    r_gap = eng.feed_depth_event({"U": last + 5, "u": last + 5, "b": [], "a": [], "E": 3})
    assert r_gap.action == "gap"

from mm.market_data.local_orderbook import LocalOrderBook


def test_topn_rolls_over_when_top_levels_cancelled():
    """
    If top-N bid levels are cancelled, deeper levels must roll up.
    The book must NOT become empty as long as deeper liquidity exists.
    """

    lob = LocalOrderBook()

    # Step 1: Seed the book with 20 bid levels and 1 ask
    # Bids: 120.0 down to 101.0
    bids = [[str(p), "1.0"] for p in range(120, 100, -1)]
    asks = [["130.0", "1.0"]]

    lob.load_snapshot(
        bids=bids,
        asks=asks,
        last_update_id=100,
    )

    # Sanity check
    top10_bids, _ = lob.top_n(10)
    assert len(top10_bids) == 10
    assert top10_bids[0][0] == 120.0
    assert top10_bids[-1][0] == 111.0

    # Step 2: Cancel the current top-10 bids
    cancel_updates = [[str(p), "0.0"] for p in range(120, 110, -1)]

    ok = lob.apply_diff(
        U=101,
        u=101,
        bids=cancel_updates,
        asks=[],
    )
    assert ok is True

    # Step 3: Ask for top-10 again
    new_top10_bids, _ = lob.top_n(10)

    # Step 4: Assertions
    assert len(new_top10_bids) == 10, "Top-10 should still be populated"
    assert new_top10_bids[0][0] == 110.0, "Best bid should roll down"
    assert new_top10_bids[-1][0] == 101.0, "10th bid should be former level 20"

    # Ensure ordering is correct (strictly decreasing prices)
    prices = [p for p, _ in new_top10_bids]
    assert prices == sorted(prices, reverse=True)

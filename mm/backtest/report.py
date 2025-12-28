from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class Summary:
    symbol: str
    n_orders: int
    n_fills: int
    buy_qty: float
    sell_qty: float
    fees: float
    start_mtm: float
    end_mtm: float
    mtm_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    end_inventory: float
    end_cash: float
    fill_rate: float
    avg_time_to_fill_ms: float
    p50_time_to_fill_ms: float
    p90_time_to_fill_ms: float


def _safe_float(s) -> float:
    try:
        return float(s)
    except Exception:
        return float("nan")


def load_outputs(out_dir: Path, symbol: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    orders_path = out_dir / f"orders_{symbol}.csv"
    fills_path = out_dir / f"fills_{symbol}.csv"
    state_path = out_dir / f"state_{symbol}.csv"

    if not orders_path.exists():
        raise FileNotFoundError(f"Missing {orders_path}")
    if not fills_path.exists():
        raise FileNotFoundError(f"Missing {fills_path}")
    if not state_path.exists():
        raise FileNotFoundError(f"Missing {state_path}")

    orders = pd.read_csv(orders_path)
    fills = pd.read_csv(fills_path)
    state = pd.read_csv(state_path)

    # basic dtype normalization
    for c in ["recv_ms", "active_recv_ms", "expire_recv_ms"]:
        if c in orders.columns:
            orders[c] = pd.to_numeric(orders[c], errors="coerce")
    for c in ["recv_ms"]:
        if c in fills.columns:
            fills[c] = pd.to_numeric(fills[c], errors="coerce")
    for c in ["recv_ms"]:
        if c in state.columns:
            state[c] = pd.to_numeric(state[c], errors="coerce")

    for c in ["price", "qty"]:
        if c in orders.columns:
            orders[c] = pd.to_numeric(orders[c], errors="coerce")
        if c in fills.columns:
            fills[c] = pd.to_numeric(fills[c], errors="coerce")

    for c in ["fee"]:
        if c in fills.columns:
            fills[c] = pd.to_numeric(fills[c], errors="coerce")

    for c in ["inventory", "cash", "mid", "mtm_value"]:
        if c in state.columns:
            state[c] = pd.to_numeric(state[c], errors="coerce")

    return orders, fills, state


def compute_time_to_fill_ms(orders: pd.DataFrame, fills: pd.DataFrame) -> pd.Series:
    if fills.empty or orders.empty:
        return pd.Series(dtype=float)

    o = orders[["order_id", "active_recv_ms"]].dropna()
    f = fills[["order_id", "recv_ms"]].dropna()
    j = f.merge(o, on="order_id", how="left")
    ttf = j["recv_ms"] - j["active_recv_ms"]
    ttf = ttf.replace([np.inf, -np.inf], np.nan).dropna()
    ttf = ttf[ttf >= 0]
    return ttf


def compute_realized_unrealized_pnl(fills: pd.DataFrame, end_mid: float) -> Tuple[float, float]:
    """
    FIFO-style realized PnL using average cost for simplicity.
    Inventory & cash are not required; we compute from fills only.
    """
    if fills.empty or np.isnan(end_mid):
        return 0.0, 0.0

    f = fills.copy()
    f = f.sort_values("recv_ms")
    # Ensure normalized side
    f["side"] = f["side"].astype(str).str.lower()

    pos = 0.0
    avg_cost = 0.0
    realized = 0.0

    for _, row in f.iterrows():
        side = row["side"]
        px = float(row["price"])
        qty = float(row["qty"])
        fee = float(row["fee"]) if "fee" in row and not pd.isna(row["fee"]) else 0.0

        if side == "buy":
            # increasing long or reducing short
            if pos >= 0:
                # add to long
                new_pos = pos + qty
                if new_pos != 0:
                    avg_cost = (avg_cost * pos + px * qty) / new_pos
                pos = new_pos
                realized -= fee
            else:
                # covering short
                cover = min(qty, -pos)
                realized += (avg_cost - px) * cover  # short profit if buy lower than avg_cost
                pos += cover
                realized -= fee
                rem = qty - cover
                if rem > 0:
                    # now open long with remaining
                    pos = rem
                    avg_cost = px
        elif side == "sell":
            if pos <= 0:
                # add to short
                new_pos = pos - qty
                if new_pos != 0:
                    # avg_cost for short position represents average sell price
                    avg_cost = (avg_cost * (-pos) + px * qty) / (-new_pos)
                pos = new_pos
                realized -= fee
            else:
                # closing long
                close = min(qty, pos)
                realized += (px - avg_cost) * close
                pos -= close
                realized -= fee
                rem = qty - close
                if rem > 0:
                    # now open short with remaining
                    pos = -rem
                    avg_cost = px
        else:
            # unknown side, ignore
            continue

    # unrealized based on remaining position
    if pos > 0:
        unrealized = (end_mid - avg_cost) * pos
    elif pos < 0:
        unrealized = (avg_cost - end_mid) * (-pos)
    else:
        unrealized = 0.0

    return float(realized), float(unrealized)


def summarize(symbol: str, orders: pd.DataFrame, fills: pd.DataFrame, state: pd.DataFrame) -> Summary:
    n_orders = int(len(orders))
    n_fills = int(len(fills))

    buy_qty = float(fills.loc[fills["side"].astype(str).str.lower() == "buy", "qty"].sum()) if n_fills else 0.0
    sell_qty = float(fills.loc[fills["side"].astype(str).str.lower() == "sell", "qty"].sum()) if n_fills else 0.0
    fees = float(fills["fee"].sum()) if (n_fills and "fee" in fills.columns) else 0.0

    start_mtm = float(state["mtm_value"].iloc[0]) if len(state) else 0.0
    end_mtm = float(state["mtm_value"].iloc[-1]) if len(state) else 0.0
    mtm_pnl = end_mtm - start_mtm

    end_mid = float(state["mid"].iloc[-1]) if len(state) else float("nan")
    realized_pnl, unrealized_pnl = compute_realized_unrealized_pnl(fills, end_mid)

    end_inventory = float(state["inventory"].iloc[-1]) if len(state) else 0.0
    end_cash = float(state["cash"].iloc[-1]) if len(state) else 0.0

    fill_rate = (n_fills / n_orders) if n_orders else 0.0

    ttf = compute_time_to_fill_ms(orders, fills)
    avg_ttf = float(ttf.mean()) if len(ttf) else float("nan")
    p50_ttf = float(ttf.quantile(0.50)) if len(ttf) else float("nan")
    p90_ttf = float(ttf.quantile(0.90)) if len(ttf) else float("nan")

    return Summary(
        symbol=symbol,
        n_orders=n_orders,
        n_fills=n_fills,
        buy_qty=buy_qty,
        sell_qty=sell_qty,
        fees=fees,
        start_mtm=start_mtm,
        end_mtm=end_mtm,
        mtm_pnl=mtm_pnl,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        end_inventory=end_inventory,
        end_cash=end_cash,
        fill_rate=fill_rate,
        avg_time_to_fill_ms=avg_ttf,
        p50_time_to_fill_ms=p50_ttf,
        p90_time_to_fill_ms=p90_ttf,
    )


def _format_ms(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "n/a"
    return f"{x:,.0f} ms"


def print_summary(s: Summary) -> None:
    print("")
    print(f"Backtest summary — {s.symbol}")
    print("-" * 72)
    print(f"Orders: {s.n_orders:,}    Fills: {s.n_fills:,}    Fill rate (fills/orders): {s.fill_rate:.3f}")
    print(f"Buy qty: {s.buy_qty:.8f}    Sell qty: {s.sell_qty:.8f}")
    print(f"Fees: {s.fees:.8f}")
    print("")
    print(f"MTM start: {s.start_mtm:.8f}    MTM end: {s.end_mtm:.8f}    MTM PnL: {s.mtm_pnl:.8f}")
    print(f"Realized PnL (avg-cost): {s.realized_pnl:.8f}    Unrealized PnL: {s.unrealized_pnl:.8f}")
    print(f"End inventory: {s.end_inventory:.8f}    End cash: {s.end_cash:.8f}")
    print("")
    print(f"Time-to-fill: avg {_format_ms(s.avg_time_to_fill_ms)} | p50 {_format_ms(s.p50_time_to_fill_ms)} | p90 {_format_ms(s.p90_time_to_fill_ms)}")
    print("-" * 72)
    print("")


def save_plots(out_dir: Path, symbol: str, orders: pd.DataFrame, fills: pd.DataFrame, state: pd.DataFrame) -> None:
    """
    Saves a small set of diagnostic plots to out_dir:
      - mtm_value over time
      - inventory over time
      - mid over time
      - fills over mid (scatter)
      - time-to-fill histogram
    """
    import matplotlib.pyplot as plt

    out_dir.mkdir(parents=True, exist_ok=True)

    if len(state):
        t = (state["recv_ms"] - state["recv_ms"].iloc[0]) / 1000.0

        plt.figure()
        plt.plot(t, state["mtm_value"])
        plt.xlabel("time (s)")
        plt.ylabel("mtm_value")
        plt.title(f"{symbol} — MTM value")
        plt.tight_layout()
        plt.savefig(out_dir / f"plot_mtm_{symbol}.png", dpi=160)
        plt.close()

        plt.figure()
        plt.plot(t, state["inventory"])
        plt.xlabel("time (s)")
        plt.ylabel("inventory")
        plt.title(f"{symbol} — Inventory")
        plt.tight_layout()
        plt.savefig(out_dir / f"plot_inventory_{symbol}.png", dpi=160)
        plt.close()

        plt.figure()
        plt.plot(t, state["mid"])
        plt.xlabel("time (s)")
        plt.ylabel("mid")
        plt.title(f"{symbol} — Mid price")
        plt.tight_layout()
        plt.savefig(out_dir / f"plot_mid_{symbol}.png", dpi=160)
        plt.close()

    if len(fills) and len(state):
        fills2 = fills.copy()
        fills2["t"] = (fills2["recv_ms"] - state["recv_ms"].iloc[0]) / 1000.0

        plt.figure()
        # plot mid as background line
        t = (state["recv_ms"] - state["recv_ms"].iloc[0]) / 1000.0
        plt.plot(t, state["mid"])
        plt.scatter(fills2["t"], fills2["price"], s=8)
        plt.xlabel("time (s)")
        plt.ylabel("price")
        plt.title(f"{symbol} — Fills over mid")
        plt.tight_layout()
        plt.savefig(out_dir / f"plot_fills_{symbol}.png", dpi=160)
        plt.close()

    ttf = compute_time_to_fill_ms(orders, fills)
    if len(ttf):
        plt.figure()
        plt.hist(ttf.values, bins=50)
        plt.xlabel("time-to-fill (ms)")
        plt.ylabel("count")
        plt.title(f"{symbol} — Time-to-fill histogram")
        plt.tight_layout()
        plt.savefig(out_dir / f"plot_ttf_{symbol}.png", dpi=160)
        plt.close()


def main() -> int:
    p = argparse.ArgumentParser(description="Summarize backtest outputs from PaperExchange.")
    p.add_argument("--out-dir", type=str, required=True, help="Directory containing orders_*.csv, fills_*.csv, state_*.csv")
    p.add_argument("--symbol", type=str, required=True, help="Symbol used in the output filenames, e.g. BTCUSDT")
    p.add_argument("--save-plots", action="store_true", help="If set, saves diagnostic plots as PNGs into out-dir")
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    symbol = args.symbol

    orders, fills, state = load_outputs(out_dir, symbol)
    s = summarize(symbol, orders, fills, state)
    print_summary(s)

    if args.save_plots:
        save_plots(out_dir, symbol, orders, fills, state)
        print(f"Saved plots to: {out_dir.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

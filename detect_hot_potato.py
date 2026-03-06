"""
Hot Potato Detection Script
============================
Reads executions.parquet and finds fill-flip candidate pairs.

A candidate pair:
  - Odd-lot execution (initiator)
  - Followed by odd-lot execution of SAME quantity, SAME stock,
    SAME price, OPPOSITE side, within the time window

Run:
    python detect_hot_potato.py
    python detect_hot_potato.py --window 1000000      # 1ms window
    python detect_hot_potato.py --stock SPY --window 5000000
"""

import polars as pl
import argparse

DEFAULT_WINDOW_NS = 10_000_000
DEFAULT_INPUT     = "executions.parquet"
DEFAULT_OUTPUT    = "fill_flip_candidates.parquet"


def detect(input_path, output_path, window_ns, stock):

    print(f"\n[Python] Loading {input_path} ...")
    df = pl.read_parquet(input_path)
    print(f"[Python] Loaded {len(df):,} rows.")
    print(f"[Python] Columns: {df.columns}")
    print(f"[Python] Sample:\n{df.head(5)}\n")

    if stock:
        df = df.filter(pl.col("stock") == stock)
        print(f"[Python] Filtered to {stock}: {len(df):,} rows.")

    if len(df) == 0:
        print("[Python] No data found. Check your symbol name.")
        return

    # Odd-lot only
    odd = df.filter(pl.col("is_odd_lot") == True)
    print(f"[Python] Odd-lot executions: {len(odd):,}")

    if len(odd) == 0:
        print("[Python] No odd-lot executions found.")
        print("         This can happen in test mode with few packets.")
        print("         Try increasing maxPackets in main.go or run on full file.")
        return

    # Build initiator and flip views
    initiator = odd.select([
        pl.col("timestamp_ns").alias("init_ts"),
        pl.col("stock").alias("init_stock"),
        pl.col("order_ref").alias("init_order_ref"),
        pl.col("side").alias("init_side"),
        pl.col("price").alias("init_price"),
        pl.col("shares_executed").alias("init_shares"),
        pl.col("match_number").alias("init_match"),
        pl.col("shares_remaining_at_level_after_fill").alias("init_depth_remaining"),
        pl.col("num_orders_remaining_at_level_after_fill").alias("init_orders_remaining"),
        pl.col("level_formed_at_ns").alias("init_level_formed_at"),
    ])

    flip = odd.select([
        pl.col("timestamp_ns").alias("flip_ts"),
        pl.col("stock").alias("flip_stock"),
        pl.col("order_ref").alias("flip_order_ref"),
        pl.col("side").alias("flip_side"),
        pl.col("price").alias("flip_price"),
        pl.col("shares_executed").alias("flip_shares"),
        pl.col("match_number").alias("flip_match"),
    ])

    print(f"[Python] Searching for pairs (window={window_ns:,} ns = {window_ns/1e6:.1f} ms)...")

    candidates = (
        initiator
        .join(
            flip,
            left_on=["init_stock", "init_price", "init_shares"],
            right_on=["flip_stock", "flip_price", "flip_shares"],
            how="inner",
        )
        .filter(pl.col("init_match") != pl.col("flip_match"))       # different event
        .filter(pl.col("flip_ts") > pl.col("init_ts"))              # flip comes after
        .filter((pl.col("flip_ts") - pl.col("init_ts")) <= window_ns)  # within window
        .filter(pl.col("init_side") == pl.col("flip_side"))         # same side — flipper hits same queue
        .with_columns(
            (pl.col("flip_ts") - pl.col("init_ts")).alias("time_delta_ns")
        )
        .sort("time_delta_ns")
        .unique(subset=["init_match"], keep="first")  # earliest flip per initiator
        .select([
            "init_ts",
            "init_stock",
            "init_order_ref",
            "init_side",
            "init_price",
            "init_shares",
            "init_match",
            "init_depth_remaining",
            "init_orders_remaining",
            "init_level_formed_at",
            "flip_ts",
            "flip_order_ref",
            "flip_side",
            "flip_match",
            "time_delta_ns",
        ])
    )

    total_pairs = len(candidates)
    total_odd   = len(odd)

    print(f"\n{'='*52}")
    print(f"  HOT POTATO DETECTION RESULTS")
    print(f"{'='*52}")
    print(f"  Window            : {window_ns:,} ns ({window_ns/1e6:.1f} ms)")
    print(f"  Total executions  : {len(df):,}")
    print(f"  Odd-lot fills     : {total_odd:,}")
    print(f"  Fill-flip pairs   : {total_pairs:,}")

    if total_odd > 0:
        print(f"  Hit rate          : {total_pairs/total_odd*100:.2f}%")

    if total_pairs > 0:
        td  = candidates["time_delta_ns"]
        dep = candidates["init_depth_remaining"]
        print(f"\n  Time delta (ns between fill and flip):")
        print(f"    min    : {td.min():,}")
        print(f"    median : {int(td.median()):,}")
        print(f"    mean   : {int(td.mean()):,}")
        print(f"    max    : {td.max():,}")
        print(f"\n  Depth remaining at initiator fill (shares):")
        print(f"    min    : {dep.min():,}")
        print(f"    median : {int(dep.median()):,}")
        print(f"    mean   : {int(dep.mean()):,}")
        print(f"    max    : {dep.max():,}")
        print(f"\n  Sample pairs:")
        print(candidates.head(5))

        candidates.write_parquet(output_path)
        print(f"\n[Python] Saved to {output_path}")
    else:
        print("\n  No pairs found.")
        print("  Try: larger window (--window 50000000)")
        print("       or run on full file (set maxPackets=0 in main.go)")

    print(f"{'='*52}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",  default=DEFAULT_INPUT)
    ap.add_argument("--output", default=DEFAULT_OUTPUT)
    ap.add_argument("--window", type=int, default=DEFAULT_WINDOW_NS)
    ap.add_argument("--stock",  default=None)
    args = ap.parse_args()
    detect(args.input, args.output, args.window, args.stock)

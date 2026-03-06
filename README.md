# Hot Potato Effect — NASDAQ ITCH 5.0 Research Pipeline

**SMMG Research** — Pankaj J. · Bhavik P. · David M.

---

## Overview

This repository contains a complete research pipeline for empirically investigating
the **hot potato effect at the intra-tick level** using NASDAQ TotalView-ITCH 5.0
full order book data.

The hot potato effect refers to the rapid inventory transfer that occurs when a
passively filled agent immediately flips their position by hitting the same price
level, passing inventory to agents behind them in the FIFO queue.

For the full methodology, findings, and analysis see:
**[`hot_potato_research.ipynb`](hot_potato_research.ipynb)**

---

## Key Results (SPY, January 30 2020)

- **80,910** SPY execution events from 423 million ITCH packets
- **37.9%** of all SPY fills were odd-lot — confirming odd-lot executions are not unusual
- **134 fill-flip candidate pairs** detected at 1ms window using fingerprint quantities
- **Minimum reaction time: 0.6 microseconds**
- **Median reaction time: 106.3 microseconds**
- **50x window expansion → only 1.54x more pairs** — strong temporal clustering
  inconsistent with random coincidence

---

## Pipeline Architecture

```
NASDAQ ITCH 5.0 Binary (.NASDAQ_ITCH50)
            │
            ▼
    Go Parser (main.go)
    ├── Live in-memory order book
    ├── Tracks depth at every price level
    └── Writes enriched execution rows
            │
            ▼
    executions.parquet
            │
            ▼
    Python Detection (detect_hot_potato.py)
    └── Fingerprint-based fill-flip detection
            │
            ▼
    Research Notebook (hot_potato_research.ipynb)
```

---

## Quick Start

### 1. Install Go dependencies
```bash
go get github.com/fraugster/parquet-go@latest
go mod tidy
```

### 2. Get ITCH data

Download free from NASDAQ EMI: https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/

**Step by step:**
1. Download any `.NASDAQ_ITCH50.gz` file (~4–6 GB compressed)
2. Decompress — Windows: use 7-Zip. Mac/Linux: `gunzip filename.gz`
3. Place the decompressed file in the `data/` folder

After this your folder should look like:
```
data/
├── PLACE_YOUR_ITCH_FILE_HERE.txt
└── 01302020.NASDAQ_ITCH50
```

### 3. Configure and run the parser

Edit the top of `main.go`:
```go
const (
    itchFilePath = `data\YOUR_FILE.NASDAQ_ITCH50`
    targetSymbol = "SPY"
    maxPackets   = 2_000_000   // set to 0 for full file
)
```

Run:
```bash
go run main.go
```

Output: `executions.parquet` — approximately 1 hour for a full trading day file.

### 4. Install Python dependencies
```bash
pip install polars pyarrow matplotlib numpy jupyter
```

### 5. Run detection
```bash
python detect_hot_potato.py                        # default 10ms window
python detect_hot_potato.py --window 1000000       # 1ms
python detect_hot_potato.py --window 50000000      # 50ms
```

### 6. Open research notebook
```bash
jupyter notebook hot_potato_research.ipynb
```

---

## Output Columns — executions.parquet

| Column | Description |
|--------|-------------|
| `timestamp_ns` | Nanoseconds since midnight |
| `stock` | Ticker |
| `order_ref` | ITCH order reference number |
| `side` | B or S — side of the resting order |
| `price` | Execution price in dollars |
| `shares_executed` | Shares filled |
| `match_number` | Unique trade identifier |
| `is_odd_lot` | True if not divisible by 100 |
| `shares_remaining_at_level_after_fill` | Queue depth after this fill |
| `num_orders_remaining_at_level_after_fill` | Orders still resting |
| `level_formed_at_ns` | When this price level first appeared |

---

## For Developers — Code Architecture

**`internal/parser/messages.go`**
Go structs for every ITCH 5.0 message type, mapping exactly to the binary spec.
To support a new ITCH version: add message structs here.

**`internal/parser/parser.go`**
Binary decoder. Reads message type byte, reads exact byte count, decodes struct.
`Decoder.Next()` returns one message per call. Also contains SoupBinTCP framing reader.

**`main.go`**
Maintains two in-memory maps:
- `orderBook` — OrderRef → {stock, side, price, originalShares, addTimestamp}
- `levelDepth` — (stock, side, price) → {sharesResting, numOrders, formedAtNs}

Level depth is read **before** the execution reduces it, then executed shares
are subtracted — giving correct post-fill depth for that specific execution.

To add a new output column: add to the Parquet schema string at the top,
compute in `writeExecution()`, add to the `AddData` map.

**`detect_hot_potato.py`**
Reads `executions.parquet`, applies fingerprint filter (quantities appearing
≤3 times at any price+side level), self-joins on (stock, price, qty, side),
applies time window filter, keeps earliest flip per initiator match number.

---

## Research Context

Developed as part of an investigation into intra-tick hot potato dynamics in
FIFO central limit order books, with regulatory implications for MiFID II
best execution monitoring.

Theoretical foundations: Ho–Stoll (1981), Glosten–Milgrom (1985),
Bodek–Shaw (2012), López de Prado.

---

## Further Research

- High-volatility regime testing: March 2020 COVID crash, August 2015 flash crash
- Individual equities alongside SPY
- European venue data (Eurex) for ESMA relevance
- Full FIFO queue position reconstruction
- Price impact analysis in microseconds following flip

---

## License

MIT — see [LICENSE](LICENSE). Use freely, modify, publish. Citation appreciated.

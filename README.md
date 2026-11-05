## ⚠️ IMPORTANT: LICENSE UPDATE NOTICE
As of June 2026, this repository and its core data pipeline ecosystem have been permanently transitioned to the **GNU Affero General Public License v3.0 (AGPL-3.0)**. 

Any ongoing use, modification, or network-based deployment of this codebase, its LOB reconstruction engine, or its event-window data outputs legally mandates that your entire software wrapper and research scripts must be made fully open-source under the same AGPL terms. Independent deployment or academic publication without explicit co-authorship and legal compliance is strictly prohibited.

# Hot Potato Effect — NASDAQ ITCH 5.0 Research Pipeline

**XAD** — Pankaj J. · Bhavik P. · David M.

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
- **134 fill-flip candidate pairs** at 1ms window under strict fingerprint criteria (qty ≥ 50, appear ≤ 3× at any price+side level)
- **400 fill-flip candidate pairs** at 1ms window under lax criteria (qty ≥ 10, not a multiple of 10)
- **Minimum reaction time: 0.6 microseconds** across both criteria
- **Median reaction time: ~106 microseconds** — consistent across both criteria
- **50x window expansion → 1.54x more pairs (strict) / 1.70x more pairs (lax)** — strong temporal clustering inconsistent with random coincidence
- Consistency of hit rates and timing across both criteria confirms the finding is robust

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
    Research Notebook (hot_potato_research.ipynb)
    ├── Fingerprint-based fill-flip detection
    ├── Strict and lax criteria comparison
    └── Full analysis, plots, and interpretation
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

### 5. Open research notebook
```bash
jupyter notebook hot_potato_research.ipynb
```

Run all cells top to bottom. Figures are saved to the `images/` folder automatically.

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

## Project Contributions & Architecture

This research pipeline was developed collaboratively to investigate the fill-flip mechanism.

* **Theory & Concept:** David M. (Originator of the fill-flip / hot-potato mechanism hypothesis)
* **Raw Data Engineering:** Bhavik P. (NASDAQ ITCH 5.0 Go parser logic and binary data handling)
* **Architecture & Reconstruction:** Pankaj J. (LOB state reconstruction, deep-book feature extraction logic, and Python detection pipeline development)

---

## License
This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**. 

See the [LICENSE](LICENSE) file for the full legal text. Any downstream use, modification, or network deployment of this codebase or its data outputs strictly requires full compliance with copyleft terms, proper author attribution, and open-sourcing of any derived works.


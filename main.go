package main

import (
	"bytes"
	"io"
	"log"
	"os"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"

	"github.com/Bhavik2205/Research_Factory.git/internal/parser"
)

// ============================================================
// CONFIGURATION — edit these before running
// ============================================================

const (
	// Path to your ITCH binary file.
	// Use full path if the file is not in this folder.
	// Examples:
	//   Windows: `C:\Users\You\data\01302020.NASDAQ_ITCH50`
	//   Relative: `data\01302020.NASDAQ_ITCH50`
	itchFilePath = `data\01302020.NASDAQ_ITCH50`

	// Where to write the output parquet file.
	outputPath = `executions.parquet`

	// Stock symbol to analyse.
	targetSymbol = "SPY"

	// Detection window in nanoseconds.
	// 1_000_000  =  1 ms
	// 5_000_000  =  5 ms
	// 10_000_000 = 10 ms
	windowSizeNs = 10_000_000

	// TEST MODE: stop after this many packets (set to 0 for full file).
	// Use a small number like 2_000_000 first to verify everything works,
	// then set to 0 to run on the full file.
	maxPackets = 2_000_000
)

// ============================================================
// ORDER BOOK STATE
// ============================================================

type orderInfo struct {
	stock          string
	side           byte
	price          uint32
	originalShares uint32
	addTimestamp   uint64
}

type levelKey struct {
	stock string
	side  byte
	price uint32
}

type levelState struct {
	sharesResting uint32
	numOrders     int
	formedAtNs    uint64
}

// ============================================================
// PARQUET SCHEMA
// ============================================================

const schemaDefinition = `
message executions {
  required int64   timestamp_ns;
  required binary  stock (STRING);
  required int64   order_ref;
  required binary  side (STRING);
  required double  price;
  required int32   shares_executed;
  required int64   match_number;
  required boolean is_odd_lot;
  required int32   shares_remaining_at_level_after_fill;
  required int32   num_orders_remaining_at_level_after_fill;
  required int64   level_formed_at_ns;
}
`

// ============================================================
// MAIN
// ============================================================

func main() {
	log.SetOutput(os.Stderr)
	log.Println("==============================================")
	log.Println(" Hot Potato Parser — NASDAQ ITCH 5.0")
	log.Println("==============================================")
	log.Printf(" Symbol      : %s", targetSymbol)
	log.Printf(" Output      : %s", outputPath)
	log.Printf(" Window (ns) : %d (%.1f ms)", windowSizeNs, float64(windowSizeNs)/1e6)
	if maxPackets > 0 {
		log.Printf(" TEST MODE   : stopping after %d packets", maxPackets)
	} else {
		log.Println(" Mode        : FULL FILE")
	}
	log.Println("==============================================")

	// --- open ITCH file ---
	f, err := os.Open(itchFilePath)
	if err != nil {
		log.Fatalf("Cannot open ITCH file: %v", err)
	}
	defer f.Close()
	log.Println("[1/3] ITCH file opened.")

	// --- create parquet output ---
	outFile, err := os.Create(outputPath)
	if err != nil {
		log.Fatalf("Cannot create output file: %v", err)
	}
	defer outFile.Close()

	sd, err := parquetschema.ParseSchemaDefinition(schemaDefinition)
	if err != nil {
		log.Fatalf("Parquet schema error: %v", err)
	}

	fw := goparquet.NewFileWriter(outFile,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(sd),
		goparquet.WithCreator("ITCH-HotPotato-Parser"),
	)
	log.Println("[2/3] Parquet writer ready.")

	// --- in-memory state ---
	orderBook   := make(map[uint64]*orderInfo, 4_000_000)
	levelDepth  := make(map[levelKey]*levelState, 100_000)

	// --- counters ---
	packetCount    := 0
	executionCount := 0
	oddLotCount    := 0

	// --- parse ---
	sbr := parser.NewSoupBinReader(f)
	log.Println("[3/3] Parsing started...")

	for {
		// Test mode cutoff
		if maxPackets > 0 && packetCount >= maxPackets {
			log.Printf("TEST MODE: reached %d packets, stopping.", maxPackets)
			break
		}

		payload, err := sbr.NextPayload()
		packetCount++

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Framing error at packet %d: %v", packetCount, err)
		}
		if payload == nil {
			continue // heartbeat
		}

		if packetCount%500_000 == 0 {
			log.Printf("  packets=%-10d  executions=%-8d  odd-lot=%-6d",
				packetCount, executionCount, oddLotCount)
		}

		dec := parser.NewDecoder(bytes.NewReader(payload))

		for {
			env, err := dec.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Decode warning (packet %d): %v", packetCount, err)
				break
			}

			switch m := env.Msg.(type) {

			// --------------------------------------------------
			// ADD ORDER — store in order book + update level
			// --------------------------------------------------
			case *parser.AddOrderNoMPIDAttribution:
				if m.Stock != targetSymbol {
					continue
				}
				orderBook[m.OrderReferenceNumber] = &orderInfo{
					stock:          m.Stock,
					side:           m.BuySellIndicator,
					price:          m.Price,
					originalShares: m.Shares,
					addTimestamp:   m.Header.Timestamp,
				}
				addToLevel(levelDepth, m.Stock, m.BuySellIndicator, m.Price, m.Shares, m.Header.Timestamp)

			case *parser.AddOrderWithMPIDAttribution:
				if m.Stock != targetSymbol {
					continue
				}
				orderBook[m.OrderReferenceNumber] = &orderInfo{
					stock:          m.Stock,
					side:           m.BuySellIndicator,
					price:          m.Price,
					originalShares: m.Shares,
					addTimestamp:   m.Header.Timestamp,
				}
				addToLevel(levelDepth, m.Stock, m.BuySellIndicator, m.Price, m.Shares, m.Header.Timestamp)

			// --------------------------------------------------
			// ORDER EXECUTED — write parquet row
			// --------------------------------------------------
			case *parser.OrderExecuted:
				info, ok := orderBook[m.OrderReferenceNumber]
				if !ok {
					continue // not our symbol
				}
				writeExecution(fw, m.Header.Timestamp, info,
					m.OrderReferenceNumber, m.ExecutedShares,
					m.MatchNumber, info.price, levelDepth)
				executionCount++
				if m.ExecutedShares%100 != 0 {
					oddLotCount++
				}
				reduceLevel(levelDepth, info, m.ExecutedShares)

			case *parser.OrderExecutedWithPrice:
				info, ok := orderBook[m.OrderReferenceNumber]
				if !ok {
					continue
				}
				execPrice := m.ExecutionPrice
				if execPrice == 0 {
					execPrice = info.price
				}
				writeExecution(fw, m.Header.Timestamp, info,
					m.OrderReferenceNumber, m.ExecutedShares,
					m.MatchNumber, execPrice, levelDepth)
				executionCount++
				if m.ExecutedShares%100 != 0 {
					oddLotCount++
				}
				reduceLevel(levelDepth, info, m.ExecutedShares)

			// --------------------------------------------------
			// ORDER CANCEL — reduce resting shares
			// --------------------------------------------------
			case *parser.OrderCancel:
				info, ok := orderBook[m.OrderReferenceNumber]
				if !ok {
					continue
				}
				lk := levelKey{stock: info.stock, side: info.side, price: info.price}
				if ls, exists := levelDepth[lk]; exists {
					if ls.sharesResting > m.CanceledShares {
						ls.sharesResting -= m.CanceledShares
					} else {
						ls.sharesResting = 0
					}
				}

			// --------------------------------------------------
			// ORDER DELETE — remove order entirely
			// --------------------------------------------------
			case *parser.OrderDelete:
				info, ok := orderBook[m.OrderReferenceNumber]
				if !ok {
					continue
				}
				lk := levelKey{stock: info.stock, side: info.side, price: info.price}
				if ls, exists := levelDepth[lk]; exists {
					ls.numOrders--
					if ls.numOrders <= 0 {
						delete(levelDepth, lk)
					}
				}
				delete(orderBook, m.OrderReferenceNumber)

			// --------------------------------------------------
			// ORDER REPLACE — remove old, add new
			// --------------------------------------------------
			case *parser.OrderReplace:
				old, ok := orderBook[m.OriginalOrderRefNumber]
				if !ok {
					continue
				}
				// remove old level contribution
				oldLk := levelKey{stock: old.stock, side: old.side, price: old.price}
				if ls, exists := levelDepth[oldLk]; exists {
					ls.numOrders--
					if ls.sharesResting >= old.originalShares {
						ls.sharesResting -= old.originalShares
					} else {
						ls.sharesResting = 0
					}
					if ls.numOrders <= 0 {
						delete(levelDepth, oldLk)
					}
				}
				delete(orderBook, m.OriginalOrderRefNumber)

				// add new order at new price
				newInfo := &orderInfo{
					stock:          old.stock,
					side:           old.side,
					price:          m.Price,
					originalShares: m.Shares,
					addTimestamp:   m.Header.Timestamp,
				}
				orderBook[m.NewOrderRefNumber] = newInfo
				addToLevel(levelDepth, old.stock, old.side, m.Price, m.Shares, m.Header.Timestamp)
			}
		}
	}

	// --- close parquet file ---
	if err := fw.Close(); err != nil {
		log.Fatalf("Error closing Parquet writer: %v", err)
	}

	log.Println("==============================================")
	log.Printf(" Packets processed : %d", packetCount)
	log.Printf(" Executions written: %d", executionCount)
	log.Printf(" Odd-lot executions: %d", oddLotCount)
	log.Printf(" Output file       : %s", outputPath)
	log.Println("==============================================")

	if executionCount == 0 {
		log.Println("WARNING: Zero executions written.")
		log.Println("  - Check that targetSymbol matches exactly (e.g. 'SPY' not 'spy')")
		log.Println("  - Check that itchFilePath points to the correct file")
		log.Println("  - If test mode: increase maxPackets, executions may come later in the file")
	}
}

// ============================================================
// HELPERS
// ============================================================

func addToLevel(
	ld map[levelKey]*levelState,
	stock string, side byte, price uint32, shares uint32, ts uint64,
) {
	lk := levelKey{stock: stock, side: side, price: price}
	if ls, exists := ld[lk]; exists {
		ls.sharesResting += shares
		ls.numOrders++
	} else {
		ld[lk] = &levelState{
			sharesResting: shares,
			numOrders:     1,
			formedAtNs:    ts,
		}
	}
}

func reduceLevel(
	ld map[levelKey]*levelState,
	info *orderInfo,
	executedShares uint32,
) {
	lk := levelKey{stock: info.stock, side: info.side, price: info.price}
	if ls, exists := ld[lk]; exists {
		if ls.sharesResting >= executedShares {
			ls.sharesResting -= executedShares
		} else {
			ls.sharesResting = 0
		}
	}
}

func writeExecution(
	fw *goparquet.FileWriter,
	ts uint64,
	info *orderInfo,
	orderRef uint64,
	executedShares uint32,
	matchNumber uint64,
	execPrice uint32,
	ld map[levelKey]*levelState,
) {
	lk := levelKey{stock: info.stock, side: info.side, price: info.price}

	var sharesRemaining int32
	var numOrdersRemaining int32
	var levelFormedAt int64

	// Read depth BEFORE reduce — then subtract executed to get post-fill state
	if ls, exists := ld[lk]; exists {
		rem := int32(ls.sharesResting) - int32(executedShares)
		if rem < 0 {
			rem = 0
		}
		sharesRemaining = rem
		numOrdersRemaining = int32(ls.numOrders)
		levelFormedAt = int64(ls.formedAtNs)
	}

	side := "S"
	if info.side == 'B' {
		side = "B"
	}

	err := fw.AddData(map[string]interface{}{
		"timestamp_ns":    int64(ts),
		"stock":           []byte(info.stock),
		"order_ref":       int64(orderRef),
		"side":            []byte(side),
		"price":           float64(execPrice) / 10_000.0,
		"shares_executed": int32(executedShares),
		"match_number":    int64(matchNumber),
		"is_odd_lot":      executedShares%100 != 0,
		"shares_remaining_at_level_after_fill":    sharesRemaining,
		"num_orders_remaining_at_level_after_fill": numOrdersRemaining,
		"level_formed_at_ns": levelFormedAt,
	})
	if err != nil {
		log.Printf("Parquet write error: %v", err)
	}
}

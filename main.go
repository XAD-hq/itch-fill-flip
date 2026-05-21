package main

import (
	"bytes"
	"io"
	"log"
	"math"
	"os"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"

	"github.com/SMMG-labs/itch-fill-flip.git/internal/parser"
)

// ============================================================
// CONFIGURATION
// ============================================================

const (
	itchFilePath     = `data\01302020.NASDAQ_ITCH50`
	execOutputPath   = `executions.parquet`
	quotesOutputPath = `quotes.parquet`
	targetSymbol     = "SPY"
	windowSizeNs     = 10_000_000
	maxPackets       = 0
)

// ============================================================
// ORDER BOOK STATE
// ============================================================

type orderInfo struct {
	stock          string
	side           byte
	price          uint32
	originalShares uint32
	currentShares  uint32 // tracks remaining shares after partial cancels
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
// QUOTE TRACKING
// ============================================================

var bidLevels = make(map[uint32]uint32, 10_000)
var askLevels = make(map[uint32]uint32, 10_000)
var lastBestBid uint32
var lastBestAsk uint32
var quoteCount uint64

func bestBid() uint32 {
	var best uint32
	for price, shares := range bidLevels {
		if shares > 0 && price > best {
			best = price
		}
	}
	return best
}

func bestAsk() uint32 {
	best := uint32(math.MaxUint32)
	found := false
	for price, shares := range askLevels {
		if shares > 0 && price < best {
			best = price
			found = true
		}
	}
	if !found {
		return 0
	}
	return best
}

func updateQuote(ts uint64, qfw *goparquet.FileWriter) {
	bb := bestBid()
	ba := bestAsk()
	if bb == lastBestBid && ba == lastBestAsk {
		return
	}
	lastBestBid = bb
	lastBestAsk = ba

	bidF := float64(bb) / 10_000.0
	askF := float64(ba) / 10_000.0

	var mid float64
	// If the book becomes one-sided, record the quotes but use NaN for mid
	if bb == 0 || ba == 0 {
		mid = math.NaN()
	} else {
		mid = (bidF + askF) / 2.0
	}

	err := qfw.AddData(map[string]interface{}{
		"timestamp_ns": int64(ts),
		"best_bid":     bidF,
		"best_ask":     askF,
		"mid":          mid,
	})
	if err != nil {
		log.Printf("Quote write error: %v", err)
	} else {
		quoteCount++
	}
}

// ============================================================
// QUOTE LEVEL HELPERS — subtract shares from bid or ask map
// ============================================================

func subtractBid(price uint32, shares uint32) {
	if bidLevels[price] > shares {
		bidLevels[price] -= shares
	} else {
		delete(bidLevels, price) // Prevents map bloat and iteration slowdown
	}
}

func subtractAsk(price uint32, shares uint32) {
	if askLevels[price] > shares {
		askLevels[price] -= shares
	} else {
		delete(askLevels, price) // Prevents map bloat and iteration slowdown
	}
}

// ============================================================
// PARQUET SCHEMAS
// ============================================================

const execSchema = `
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

const quotesSchema = `
message quotes {
  required int64  timestamp_ns;
  required double best_bid;
  required double best_ask;
  required double mid;
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
	log.Printf(" Symbol  : %s", targetSymbol)
	log.Printf(" Outputs : %s  %s", execOutputPath, quotesOutputPath)
	if maxPackets > 0 {
		log.Printf(" TEST MODE: stopping after %d packets", maxPackets)
	} else {
		log.Println(" Mode    : FULL FILE")
	}
	log.Println("==============================================")

	f, err := os.Open(itchFilePath)
	if err != nil {
		log.Fatalf("Cannot open ITCH file: %v", err)
	}
	defer f.Close()

	outFile, err := os.Create(execOutputPath)
	if err != nil {
		log.Fatalf("Cannot create executions file: %v", err)
	}
	defer outFile.Close()

	sd, err := parquetschema.ParseSchemaDefinition(execSchema)
	if err != nil {
		log.Fatalf("Exec schema error: %v", err)
	}
	efw := goparquet.NewFileWriter(outFile,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(sd),
		goparquet.WithCreator("ITCH-HotPotato-Parser"),
	)

	qFile, err := os.Create(quotesOutputPath)
	if err != nil {
		log.Fatalf("Cannot create quotes file: %v", err)
	}
	defer qFile.Close()

	qsd, err := parquetschema.ParseSchemaDefinition(quotesSchema)
	if err != nil {
		log.Fatalf("Quotes schema error: %v", err)
	}
	qfw := goparquet.NewFileWriter(qFile,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(qsd),
		goparquet.WithCreator("ITCH-HotPotato-Parser"),
	)

	orderBook := make(map[uint64]*orderInfo, 4_000_000)
	levelDepth := make(map[levelKey]*levelState, 100_000)

	packetCount := 0
	executionCount := 0
	oddLotCount := 0

	sbr := parser.NewSoupBinReader(f)
	log.Println("Parsing started...")

	for {
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
			continue
		}

		if packetCount%500_000 == 0 {
			log.Printf("  packets=%-10d  executions=%-8d  quotes=%-8d  odd-lot=%-6d",
				packetCount, executionCount, quoteCount, oddLotCount)
		}

		dec := parser.NewDecoder(bytes.NewReader(payload))

		for {
			env, err := dec.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			switch m := env.Msg.(type) {

			// ── ADD ORDER ──────────────────────────────────────────
			case *parser.AddOrderNoMPIDAttribution:
				if m.Stock != targetSymbol {
					continue
				}
				orderBook[m.OrderReferenceNumber] = &orderInfo{
					stock: m.Stock, side: m.BuySellIndicator,
					price: m.Price, originalShares: m.Shares,
					currentShares: m.Shares,
					addTimestamp:  m.Header.Timestamp,
				}
				addToLevel(levelDepth, m.Stock, m.BuySellIndicator, m.Price, m.Shares, m.Header.Timestamp)
				if m.BuySellIndicator == 'B' {
					bidLevels[m.Price] += m.Shares
				} else {
					askLevels[m.Price] += m.Shares
				}
				updateQuote(m.Header.Timestamp, qfw)

			case *parser.AddOrderWithMPIDAttribution:
				if m.Stock != targetSymbol {
					continue
				}
				orderBook[m.OrderReferenceNumber] = &orderInfo{
					stock: m.Stock, side: m.BuySellIndicator,
					price: m.Price, originalShares: m.Shares,
					currentShares: m.Shares,
					addTimestamp:  m.Header.Timestamp,
				}
				addToLevel(levelDepth, m.Stock, m.BuySellIndicator, m.Price, m.Shares, m.Header.Timestamp)
				if m.BuySellIndicator == 'B' {
					bidLevels[m.Price] += m.Shares
				} else {
					askLevels[m.Price] += m.Shares
				}
				updateQuote(m.Header.Timestamp, qfw)

			// ── ORDER EXECUTED ─────────────────────────────────────
			case *parser.OrderExecuted:
				info, ok := orderBook[m.OrderReferenceNumber]
				if !ok {
					continue
				}

				// Determine if order will die *before* reducing it
				isDead := info.currentShares <= m.ExecutedShares

				writeExecution(efw, m.Header.Timestamp, info,
					m.OrderReferenceNumber, m.ExecutedShares,
					m.MatchNumber, info.price, levelDepth, isDead)
				executionCount++

				if m.ExecutedShares%100 != 0 {
					oddLotCount++
				}

				if info.currentShares >= m.ExecutedShares {
					info.currentShares -= m.ExecutedShares
				} else {
					info.currentShares = 0
				}

				reduceLevel(levelDepth, info, m.ExecutedShares, isDead)

				if info.side == 'B' {
					subtractBid(info.price, m.ExecutedShares)
				} else {
					subtractAsk(info.price, m.ExecutedShares)
				}

				if isDead {
					delete(orderBook, m.OrderReferenceNumber)
				}
				updateQuote(m.Header.Timestamp, qfw)

			case *parser.OrderExecutedWithPrice:
				info, ok := orderBook[m.OrderReferenceNumber]
				if !ok {
					continue
				}
				execPrice := m.ExecutionPrice
				if execPrice == 0 {
					execPrice = info.price
				}

				isDead := info.currentShares <= m.ExecutedShares

				writeExecution(efw, m.Header.Timestamp, info,
					m.OrderReferenceNumber, m.ExecutedShares,
					m.MatchNumber, execPrice, levelDepth, isDead)
				executionCount++

				if m.ExecutedShares%100 != 0 {
					oddLotCount++
				}

				if info.currentShares >= m.ExecutedShares {
					info.currentShares -= m.ExecutedShares
				} else {
					info.currentShares = 0
				}

				reduceLevel(levelDepth, info, m.ExecutedShares, isDead)

				if info.side == 'B' {
					subtractBid(info.price, m.ExecutedShares)
				} else {
					subtractAsk(info.price, m.ExecutedShares)
				}

				if isDead {
					delete(orderBook, m.OrderReferenceNumber)
				}
				updateQuote(m.Header.Timestamp, qfw)

			// ── ORDER CANCEL ───────────────────────────────────────
			case *parser.OrderCancel:
				info, ok := orderBook[m.OrderReferenceNumber]
				if !ok {
					continue
				}
				canceled := m.CanceledShares
				if info.currentShares >= canceled {
					info.currentShares -= canceled
				} else {
					canceled = info.currentShares
					info.currentShares = 0
				}

				isDead := info.currentShares == 0
				reduceLevel(levelDepth, info, canceled, isDead)

				if info.side == 'B' {
					subtractBid(info.price, canceled)
				} else {
					subtractAsk(info.price, canceled)
				}

				if isDead {
					delete(orderBook, m.OrderReferenceNumber)
				}
				updateQuote(m.Header.Timestamp, qfw)

			// ── ORDER DELETE ───────────────────────────────────────
			case *parser.OrderDelete:
				info, ok := orderBook[m.OrderReferenceNumber]
				if !ok {
					continue
				}
				remaining := info.currentShares

				reduceLevel(levelDepth, info, remaining, true) // isDead is always true here

				if info.side == 'B' {
					subtractBid(info.price, remaining)
				} else {
					subtractAsk(info.price, remaining)
				}

				delete(orderBook, m.OrderReferenceNumber)
				updateQuote(m.Header.Timestamp, qfw)

			// ── ORDER REPLACE ──────────────────────────────────────
			case *parser.OrderReplace:
				old, ok := orderBook[m.OriginalOrderRefNumber]
				if !ok {
					continue
				}

				// 1. Remove old order logic completely
				oldRemaining := old.currentShares
				reduceLevel(levelDepth, old, oldRemaining, true) // isDead is always true for the old order

				if old.side == 'B' {
					subtractBid(old.price, oldRemaining)
				} else {
					subtractAsk(old.price, oldRemaining)
				}
				delete(orderBook, m.OriginalOrderRefNumber)

				// 2. Add new order at new price with new shares
				newInfo := &orderInfo{
					stock:          old.stock,
					side:           old.side,
					price:          m.Price,
					originalShares: m.Shares,
					currentShares:  m.Shares,
					addTimestamp:   m.Header.Timestamp,
				}
				orderBook[m.NewOrderRefNumber] = newInfo
				addToLevel(levelDepth, old.stock, old.side, m.Price, m.Shares, m.Header.Timestamp)
				if old.side == 'B' {
					bidLevels[m.Price] += m.Shares
				} else {
					askLevels[m.Price] += m.Shares
				}
				updateQuote(m.Header.Timestamp, qfw)
			}
		}
	}

	if err := efw.Close(); err != nil {
		log.Fatalf("Error closing executions writer: %v", err)
	}
	if err := qfw.Close(); err != nil {
		log.Fatalf("Error closing quotes writer: %v", err)
	}

	log.Println("==============================================")
	log.Printf(" Packets processed : %d", packetCount)
	log.Printf(" Executions written: %d", executionCount)
	log.Printf(" Odd-lot executions: %d", oddLotCount)
	log.Printf(" Quote rows written: %d", quoteCount)
	log.Printf(" Outputs: %s  %s", execOutputPath, quotesOutputPath)
	log.Println("==============================================")
}

// ============================================================
// HELPERS
// ============================================================

func addToLevel(ld map[levelKey]*levelState, stock string, side byte, price uint32, shares uint32, ts uint64) {
	lk := levelKey{stock: stock, side: side, price: price}
	if ls, exists := ld[lk]; exists {
		ls.sharesResting += shares
		ls.numOrders++
	} else {
		ld[lk] = &levelState{sharesResting: shares, numOrders: 1, formedAtNs: ts}
	}
}

// Updated reduceLevel to strictly handle numOrders decrements and level deletion
func reduceLevel(ld map[levelKey]*levelState, info *orderInfo, executedShares uint32, isDead bool) {
	lk := levelKey{stock: info.stock, side: info.side, price: info.price}
	if ls, exists := ld[lk]; exists {
		if ls.sharesResting >= executedShares {
			ls.sharesResting -= executedShares
		} else {
			ls.sharesResting = 0
		}

		if isDead {
			ls.numOrders--
		}

		// Clean up the level if it is completely empty
		if ls.numOrders <= 0 || ls.sharesResting == 0 {
			delete(ld, lk)
		}
	}
}

// Updated writeExecution to properly track orders remaining when an order dies
func writeExecution(fw *goparquet.FileWriter, ts uint64, info *orderInfo, orderRef uint64,
	executedShares uint32, matchNumber uint64, execPrice uint32, ld map[levelKey]*levelState, isDead bool) {

	lk := levelKey{stock: info.stock, side: info.side, price: info.price}
	var sharesRemaining int32
	var numOrdersRemaining int32
	var levelFormedAt int64

	if ls, exists := ld[lk]; exists {
		rem := int32(ls.sharesResting) - int32(executedShares)
		if rem < 0 {
			rem = 0
		}
		sharesRemaining = rem
		numOrdersRemaining = int32(ls.numOrders)

		// If the execution kills the order, the post-fill level count must reflect that
		if isDead {
			numOrdersRemaining--
			if numOrdersRemaining < 0 {
				numOrdersRemaining = 0
			}
		}
		levelFormedAt = int64(ls.formedAtNs)
	}

	side := "S"
	if info.side == 'B' {
		side = "B"
	}

	err := fw.AddData(map[string]interface{}{
		"timestamp_ns":                         int64(ts),
		"stock":                                []byte(info.stock),
		"order_ref":                            int64(orderRef),
		"side":                                 []byte(side),
		"price":                                float64(execPrice) / 10_000.0,
		"shares_executed":                      int32(executedShares),
		"match_number":                         int64(matchNumber),
		"is_odd_lot":                           executedShares%100 != 0,
		"shares_remaining_at_level_after_fill": sharesRemaining,
		"num_orders_remaining_at_level_after_fill": numOrdersRemaining,
		"level_formed_at_ns":                       levelFormedAt,
	})
	if err != nil {
		log.Printf("Parquet write error: %v", err)
	}
}

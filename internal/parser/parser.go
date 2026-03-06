package parser

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// SoupBinReader reads SoupBinTCP frames and returns ITCH payloads.
type SoupBinReader struct {
	r io.Reader
}

func NewSoupBinReader(r io.Reader) *SoupBinReader {
	return &SoupBinReader{r: r}
}

func (s *SoupBinReader) NextPayload() ([]byte, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(s.r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint16(lenBuf[:])
	if n == 0 {
		return nil, nil // heartbeat
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(s.r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

type Envelope struct {
	Type byte
	Msg  any
}

var ErrUnknownMessageType = errors.New("itch5: unknown message type")

const headerLen = 11

var msgLen = map[byte]int{
	'S': 12,
	'R': 39,
	'H': 25,
	'Y': 20,
	'L': 26,
	'V': 35,
	'W': 12,
	'K': 28,
	'J': 35,
	'h': 21,
	'A': 36,
	'F': 40,
	'E': 31,
	'C': 36,
	'X': 23,
	'D': 19,
	'U': 35,
	'P': 44,
	'Q': 40,
	'B': 19,
	'I': 50,
	'N': 20,
	'O': 48,
}

var maxMsgLen int

func init() {
	for _, n := range msgLen {
		if n > maxMsgLen {
			maxMsgLen = n
		}
	}
}

type Decoder struct {
	br  *bufio.Reader
	buf []byte
}

func NewDecoder(r io.Reader) *Decoder {
	if br, ok := r.(*bufio.Reader); ok {
		return &Decoder{br: br, buf: make([]byte, maxMsgLen-1)}
	}
	return &Decoder{br: bufio.NewReaderSize(r, 64*1024), buf: make([]byte, maxMsgLen-1)}
}

func (d *Decoder) Next() (Envelope, error) {
	var env Envelope

	t, err := d.br.ReadByte()
	if err != nil {
		return env, err
	}
	l := msgLen[t]
	if l == 0 {
		return env, fmt.Errorf("%w: 0x%02x", ErrUnknownMessageType, t)
	}

	n := l - 1
	buf := d.buf[:n]
	if _, err := io.ReadFull(d.br, buf); err != nil {
		return env, err
	}

	if n < (headerLen - 1) {
		return env, io.ErrUnexpectedEOF
	}
	h := MessageHeader{
		MessageType:    t,
		StockLocate:    beU16(buf[0:2]),
		TrackingNumber: beU16(buf[2:4]),
		Timestamp:      beU48(buf[4:10]),
	}
	p := buf[10:]

	switch t {
	case 'S':
		if len(p) != 1 {
			return env, shortPayload("SystemEvent", 1, len(p))
		}
		return Envelope{Type: t, Msg: &SystemEvent{Header: h, EventCode: p[0]}}, nil

	case 'R':
		if len(p) != 28 {
			return env, shortPayload("StockDirectory", 28, len(p))
		}
		m := &StockDirectory{Header: h}
		off := 0
		m.Stock = asciiTrim(p[off : off+8]); off += 8
		m.MarketCategory = p[off]; off++
		m.FinancialStatusIndicator = p[off]; off++
		m.RoundLotSize = beU32(p[off : off+4]); off += 4
		m.RoundLotsOnly = p[off]; off++
		m.IssueClassification = p[off]; off++
		m.IssueSubType = asciiTrim(p[off : off+2]); off += 2
		m.Authenticity = p[off]; off++
		m.ShortSaleThreshold = p[off]; off++
		m.IPOFlag = p[off]; off++
		m.LULDReferencePriceTier = p[off]; off++
		m.ETPFlag = p[off]; off++
		m.ETPLeverageFactor = beU32(p[off : off+4]); off += 4
		m.InverseIndicator = p[off]
		return Envelope{Type: t, Msg: m}, nil

	case 'H':
		if len(p) != 14 {
			return env, shortPayload("StockTradingAction", 14, len(p))
		}
		m := &StockTradingAction{Header: h}
		m.Stock = asciiTrim(p[0:8])
		m.TradingState = p[8]
		m.Reserved = p[9]
		m.Reason = asciiTrim(p[10:14])
		return Envelope{Type: t, Msg: m}, nil

	case 'Y':
		if len(p) != 9 {
			return env, shortPayload("RegSHO", 9, len(p))
		}
		m := &RegSHOShortSalePriceTestRestrictedIndicator{Header: h}
		m.Stock = asciiTrim(p[0:8])
		m.RegSHOAction = p[8]
		return Envelope{Type: t, Msg: m}, nil

	case 'L':
		if len(p) != 15 {
			return env, shortPayload("MarketParticipantPosition", 15, len(p))
		}
		m := &MarketParticipantPosition{Header: h}
		m.MPID = asciiTrim(p[0:4])
		m.Stock = asciiTrim(p[4:12])
		m.PrimaryMarketMaker = p[12]
		m.MarketMakerMode = p[13]
		m.MarketParticipantState = p[14]
		return Envelope{Type: t, Msg: m}, nil

	case 'V':
		if len(p) != 24 {
			return env, shortPayload("MWCBDeclineLevel", 24, len(p))
		}
		m := &MWCBDeclineLevel{Header: h}
		m.Level1 = beU64(p[0:8])
		m.Level2 = beU64(p[8:16])
		m.Level3 = beU64(p[16:24])
		return Envelope{Type: t, Msg: m}, nil

	case 'W':
		if len(p) != 1 {
			return env, shortPayload("MWCBStatus", 1, len(p))
		}
		return Envelope{Type: t, Msg: &MWCBStatus{Header: h, BreachedLevel: p[0]}}, nil

	case 'K':
		if len(p) != 17 {
			return env, shortPayload("QuotingPeriodUpdate", 17, len(p))
		}
		m := &QuotingPeriodUpdate{Header: h}
		m.Stock = asciiTrim(p[0:8])
		m.IPOQuotationReleaseTime = beU32(p[8:12])
		m.IPOQuotationReleaseQualifier = p[12]
		m.IPOPrice = beU32(p[13:17])
		return Envelope{Type: t, Msg: m}, nil

	case 'J':
		if len(p) != 24 {
			return env, shortPayload("LULDAuctionCollar", 24, len(p))
		}
		m := &LULDAuctionCollar{Header: h}
		m.Stock = asciiTrim(p[0:8])
		m.AuctionCollarReferencePrice = beU32(p[8:12])
		m.UpperAuctionCollarPrice = beU32(p[12:16])
		m.LowerAuctionCollarPrice = beU32(p[16:20])
		m.AuctionCollarExtension = beU32(p[20:24])
		return Envelope{Type: t, Msg: m}, nil

	case 'h':
		if len(p) != 10 {
			return env, shortPayload("OperationalHalt", 10, len(p))
		}
		m := &OperationalHalt{Header: h}
		m.Stock = asciiTrim(p[0:8])
		m.MarketCode = p[8]
		m.OperationalHaltAction = p[9]
		return Envelope{Type: t, Msg: m}, nil

	case 'A':
		if len(p) != 25 {
			return env, shortPayload("AddOrderNoMPID", 25, len(p))
		}
		m := &AddOrderNoMPIDAttribution{Header: h}
		m.OrderReferenceNumber = beU64(p[0:8])
		m.BuySellIndicator = p[8]
		m.Shares = beU32(p[9:13])
		m.Stock = asciiTrim(p[13:21])
		m.Price = beU32(p[21:25])
		return Envelope{Type: t, Msg: m}, nil

	case 'F':
		if len(p) != 29 {
			return env, shortPayload("AddOrderWithMPID", 29, len(p))
		}
		m := &AddOrderWithMPIDAttribution{Header: h}
		m.OrderReferenceNumber = beU64(p[0:8])
		m.BuySellIndicator = p[8]
		m.Shares = beU32(p[9:13])
		m.Stock = asciiTrim(p[13:21])
		m.Price = beU32(p[21:25])
		m.Attribution = asciiTrim(p[25:29])
		return Envelope{Type: t, Msg: m}, nil

	case 'E':
		if len(p) != 20 {
			return env, shortPayload("OrderExecuted", 20, len(p))
		}
		m := &OrderExecuted{Header: h}
		m.OrderReferenceNumber = beU64(p[0:8])
		m.ExecutedShares = beU32(p[8:12])
		m.MatchNumber = beU64(p[12:20])
		return Envelope{Type: t, Msg: m}, nil

	case 'C':
		if len(p) != 25 {
			return env, shortPayload("OrderExecutedWithPrice", 25, len(p))
		}
		m := &OrderExecutedWithPrice{Header: h}
		m.OrderReferenceNumber = beU64(p[0:8])
		m.ExecutedShares = beU32(p[8:12])
		m.MatchNumber = beU64(p[12:20])
		m.Printable = p[20]
		m.ExecutionPrice = beU32(p[21:25])
		return Envelope{Type: t, Msg: m}, nil

	case 'X':
		if len(p) != 12 {
			return env, shortPayload("OrderCancel", 12, len(p))
		}
		m := &OrderCancel{Header: h}
		m.OrderReferenceNumber = beU64(p[0:8])
		m.CanceledShares = beU32(p[8:12])
		return Envelope{Type: t, Msg: m}, nil

	case 'D':
		if len(p) != 8 {
			return env, shortPayload("OrderDelete", 8, len(p))
		}
		m := &OrderDelete{Header: h}
		m.OrderReferenceNumber = beU64(p[0:8])
		return Envelope{Type: t, Msg: m}, nil

	case 'U':
		if len(p) != 24 {
			return env, shortPayload("OrderReplace", 24, len(p))
		}
		m := &OrderReplace{Header: h}
		m.OriginalOrderRefNumber = beU64(p[0:8])
		m.NewOrderRefNumber = beU64(p[8:16])
		m.Shares = beU32(p[16:20])
		m.Price = beU32(p[20:24])
		return Envelope{Type: t, Msg: m}, nil

	case 'P':
		if len(p) != 33 {
			return env, shortPayload("TradeNonCross", 33, len(p))
		}
		m := &TradeMessageNonCross{Header: h}
		m.OrderReferenceNumber = beU64(p[0:8])
		m.BuySellIndicator = p[8]
		m.Shares = beU32(p[9:13])
		m.Stock = asciiTrim(p[13:21])
		m.Price = beU32(p[21:25])
		m.MatchNumber = beU64(p[25:33])
		return Envelope{Type: t, Msg: m}, nil

	case 'Q':
		if len(p) != 29 {
			return env, shortPayload("CrossTrade", 29, len(p))
		}
		m := &CrossTradeMessage{Header: h}
		m.Shares = beU64(p[0:8])
		m.Stock = asciiTrim(p[8:16])
		m.CrossPrice = beU32(p[16:20])
		m.MatchNumber = beU64(p[20:28])
		m.CrossType = p[28]
		return Envelope{Type: t, Msg: m}, nil

	case 'B':
		if len(p) != 8 {
			return env, shortPayload("BrokenTrade", 8, len(p))
		}
		m := &BrokenTradeMessage{Header: h}
		m.MatchNumber = beU64(p[0:8])
		return Envelope{Type: t, Msg: m}, nil

	case 'I':
		if len(p) != 39 {
			return env, shortPayload("NOII", 39, len(p))
		}
		m := &NetOrderImbalanceIndicator{Header: h}
		m.PairedShares = beU64(p[0:8])
		m.ImbalanceShares = beU64(p[8:16])
		m.ImbalanceDirection = p[16]
		m.Stock = asciiTrim(p[17:25])
		m.FarPrice = beU32(p[25:29])
		m.NearPrice = beU32(p[29:33])
		m.CurrentReferencePrice = beU32(p[33:37])
		m.CrossType = p[37]
		m.PriceVariationIndicator = p[38]
		return Envelope{Type: t, Msg: m}, nil

	case 'N':
		if len(p) != 9 {
			return env, shortPayload("RetailPriceImprovement", 9, len(p))
		}
		m := &RetailPriceImprovementIndicator{Header: h}
		m.Stock = asciiTrim(p[0:8])
		m.InterestFlag = p[8]
		return Envelope{Type: t, Msg: m}, nil

	case 'O':
		if len(p) != 37 {
			return env, shortPayload("DirectListing", 37, len(p))
		}
		m := &DirectListingWithCapitalRaisePriceDiscovery{Header: h}
		m.Stock = asciiTrim(p[0:8])
		m.OpenEligibilityStatus = p[8]
		m.MinimumAllowablePrice = beU32(p[9:13])
		m.MaximumAllowablePrice = beU32(p[13:17])
		m.NearExecutionPrice = beU32(p[17:21])
		m.NearExecutionTime = beU64(p[21:29])
		m.LowerPriceRangeCollar = beU32(p[29:33])
		m.UpperPriceRangeCollar = beU32(p[33:37])
		return Envelope{Type: t, Msg: m}, nil
	}

	return env, fmt.Errorf("%w: 0x%02x", ErrUnknownMessageType, t)
}

func beU16(b []byte) uint16 { return binary.BigEndian.Uint16(b) }
func beU32(b []byte) uint32 { return binary.BigEndian.Uint32(b) }
func beU64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

func beU48(b []byte) uint64 {
	_ = b[5]
	return (uint64(b[0]) << 40) | (uint64(b[1]) << 32) | (uint64(b[2]) << 24) |
		(uint64(b[3]) << 16) | (uint64(b[4]) << 8) | uint64(b[5])
}

func asciiTrim(b []byte) string {
	i := len(b) - 1
	for i >= 0 && b[i] == ' ' {
		i--
	}
	return string(b[:i+1])
}

func shortPayload(name string, want, got int) error {
	return fmt.Errorf("itch5: %s payload length mismatch: want %d got %d", name, want, got)
}

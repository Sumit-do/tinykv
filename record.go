package tinykv

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// OpType identifies the kind of operation a log record describes.
type OpType uint8

const (
	// OpSet writes or overwrites a key.
	OpSet OpType = 1
	// OpDelete removes a key (a tombstone record).
	OpDelete OpType = 2
)

// On-disk record layout (all integers little-endian):
//
//	+----------+----------+--------+----------------+---------+-------+---------+
//	| len u32  | crc32 u32| op u8  | expires i64    | klen u32| key   | value   |
//	+----------+----------+--------+----------------+---------+-------+---------+
//	|<- header (8 bytes) ->|<------------- payload (len bytes) ------------------>|
//
// len is the payload length. crc32 (IEEE) covers the payload only.
// expires is a Unix timestamp in nanoseconds; 0 means "never expires".
// Delete records carry an empty value.
const (
	headerSize  = 8  // len (4) + crc (4)
	payloadMeta = 13 // op (1) + expires (8) + klen (4)

	// MaxKeySize is the largest key accepted, in bytes.
	MaxKeySize = 1 << 16
	// MaxValueSize is the largest value accepted, in bytes.
	MaxValueSize = 1 << 30

	maxPayload = payloadMeta + MaxKeySize + MaxValueSize
)

var (
	// ErrCorrupt is returned when a record fails its checksum or structural checks.
	ErrCorrupt = errors.New("tinykv: corrupt record")

	errTruncated = errors.New("tinykv: truncated record")
)

type record struct {
	op      OpType
	expires int64
	key     string
	value   []byte
}

// size returns the encoded size of the record on disk.
func (r *record) size() int { return headerSize + payloadMeta + len(r.key) + len(r.value) }

// appendTo encodes r and appends the bytes to buf.
func (r *record) appendTo(buf []byte) []byte {
	payloadLen := payloadMeta + len(r.key) + len(r.value)
	start := len(buf)
	buf = append(buf, make([]byte, headerSize+payloadLen)...)
	b := buf[start:]
	binary.LittleEndian.PutUint32(b[0:4], uint32(payloadLen))
	p := b[headerSize:]
	p[0] = byte(r.op)
	binary.LittleEndian.PutUint64(p[1:9], uint64(r.expires))
	binary.LittleEndian.PutUint32(p[9:13], uint32(len(r.key)))
	copy(p[payloadMeta:], r.key)
	copy(p[payloadMeta+len(r.key):], r.value)
	binary.LittleEndian.PutUint32(b[4:8], crc32.ChecksumIEEE(p))
	return buf
}

// decodePayload parses a checksummed payload. The returned value aliases p.
func decodePayload(p []byte) (record, error) {
	if len(p) < payloadMeta {
		return record{}, ErrCorrupt
	}
	op := OpType(p[0])
	if op != OpSet && op != OpDelete {
		return record{}, ErrCorrupt
	}
	klen := int(binary.LittleEndian.Uint32(p[9:13]))
	if klen == 0 || klen > MaxKeySize || payloadMeta+klen > len(p) {
		return record{}, ErrCorrupt
	}
	return record{
		op:      op,
		expires: int64(binary.LittleEndian.Uint64(p[1:9])),
		key:     string(p[payloadMeta : payloadMeta+klen]),
		value:   p[payloadMeta+klen:],
	}, nil
}

// readRecord reads one record from r, using *scratch as a reusable buffer.
// It returns the decoded record and its total on-disk size. At a clean end
// of file it returns io.EOF; a partially written record yields errTruncated
// and a checksum/structure failure yields ErrCorrupt.
func readRecord(r io.Reader, scratch *[]byte) (record, int, error) {
	if cap(*scratch) < headerSize {
		*scratch = make([]byte, 0, 4096)
	}
	hdr := (*scratch)[:headerSize]
	if _, err := io.ReadFull(r, hdr); err != nil {
		if err == io.EOF {
			return record{}, 0, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			return record{}, 0, errTruncated
		}
		return record{}, 0, err
	}
	n := binary.LittleEndian.Uint32(hdr[0:4])
	crc := binary.LittleEndian.Uint32(hdr[4:8])
	if n < payloadMeta || n > maxPayload {
		return record{}, 0, ErrCorrupt
	}
	if cap(*scratch) < int(n) {
		*scratch = make([]byte, 0, int(n)*2)
	}
	p := (*scratch)[:n]
	if _, err := io.ReadFull(r, p); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return record{}, 0, errTruncated
		}
		return record{}, 0, err
	}
	if crc32.ChecksumIEEE(p) != crc {
		return record{}, 0, ErrCorrupt
	}
	rec, err := decodePayload(p)
	return rec, headerSize + int(n), err
}

// verifyRecord checks that b holds exactly one well-formed record.
func verifyRecord(b []byte) error {
	if len(b) < headerSize+payloadMeta {
		return ErrCorrupt
	}
	n := binary.LittleEndian.Uint32(b[0:4])
	if int(n) != len(b)-headerSize {
		return ErrCorrupt
	}
	if crc32.ChecksumIEEE(b[headerSize:]) != binary.LittleEndian.Uint32(b[4:8]) {
		return ErrCorrupt
	}
	return nil
}

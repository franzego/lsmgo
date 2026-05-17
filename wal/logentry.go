package wal

import (
	"encoding/binary"
	"errors"
)

type LogEntry struct {
	SeqNum uint64
	Count  uint32
	Data   []byte // raw batch.Repr() bytes, header already contains SeqNum and Count
}

var ErrCorruptEntry = errors.New("error: corrupt logentry")

// DecodeEntry takes a reassembled data blob from the physical record
// reader and reconstructs a LogEntry from it.
func DecodeEntry(data []byte) (*LogEntry, error) {
	if len(data) < 12 {
		return nil, ErrCorruptEntry
	}
	return &LogEntry{
		SeqNum: binary.LittleEndian.Uint64(data[0:8]),
		Count:  binary.LittleEndian.Uint32(data[8:12]),
		Data:   data,
	}, nil
}

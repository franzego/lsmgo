package log

import (
	"errors"
	"hash/crc32"
)

const (
	BlockSize        = 32 * 1024
	RecordHeaderSize = 7
)

type RecordType uint8

const (
	RecordFull   RecordType = 1
	RecordFirst  RecordType = 2
	RecordMiddle RecordType = 3
	RecordLast   RecordType = 4
)

var (
	crcTable         = crc32.MakeTable(crc32.Castagnoli)
	ErrCorruptRecord = errors.New("log: corrupt record")
)

func ComputeChecksum(rt RecordType, data []byte) uint32 {
	crc := crc32.New(crcTable)
	crc.Write([]byte{byte(rt)})
	crc.Write(data)
	return crc.Sum32()
}

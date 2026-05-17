package wal

import (
	"hash/crc32"
)

const (
	blocksize  = 32 * 1024 // this is for the WAL
	headerSize = 7
)

type RecordType uint8

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func computeChecksum(rt RecordType, data []byte) uint32 {
	crc := crc32.New(crcTable)
	crc.Write([]byte{byte(rt)})
	crc.Write(data)
	return crc.Sum32()

}

const (
	RecordFull   RecordType = 1
	RecordFirst  RecordType = 2
	RecordMiddle RecordType = 3
	RecordLast   RecordType = 4
)

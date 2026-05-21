package wal

import (
	"hash/crc32"
)

const (
	blockSize        = 32 * 1024 // this is for organisation in the WAL
	recordHeaderSize = 7
)

type RecordType uint8

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func computeChecksum(rt RecordType, data []byte) uint32 {
	crc := crc32.New(crcTable)
	crc.Write([]byte{byte(rt)})
	crc.Write(data)
	return crc.Sum32()

}

// This is required for the reader to tell if the bytes it is reading in the WAL
// is: 1. A complete batch that fitted into a single 32KB block 2. The beginning
// of a batch entry that did not fit into a single block and continues in the
// next one 3. The end of a batch - A completed entry. A RecordFull means that it
// is a single complete entry that fit into a single block. A RecordFirst
// signifies the start of a logentry (batch) that did not fit into a single 32KB
// block. A RecordMiddle signifies "keep accumulating". A RecordLast signifies
// the final fragment. It means the logentry has been completed and can now be
// committed.
const (
	RecordFull   RecordType = 1
	RecordFirst  RecordType = 2
	RecordMiddle RecordType = 3
	RecordLast   RecordType = 4
)

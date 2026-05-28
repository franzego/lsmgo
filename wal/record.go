package wal

import (
	ilog "github.com/franzego/lsm-golang/internal/log"
)

const (
	blockSize        = ilog.BlockSize
	recordHeaderSize = ilog.RecordHeaderSize
)

type RecordType = ilog.RecordType

func computeChecksum(rt RecordType, data []byte) uint32 {
	return ilog.ComputeChecksum(rt, data)
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
	RecordFull   = ilog.RecordFull
	RecordFirst  = ilog.RecordFirst
	RecordMiddle = ilog.RecordMiddle
	RecordLast   = ilog.RecordLast
)

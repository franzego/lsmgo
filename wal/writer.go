package wal

import (
	"encoding/binary"
)

func (w *WAL) Write(batchData []byte, rt RecordType) error {
	// build the record file that will be written to the disk (WAL)
	checksum := computeChecksum(rt, batchData)
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint16(header[4:6], uint16(len(batchData)))
	header[6] = byte(rt)
	record := make([]byte, headerSize+len(batchData))
	copy(record[0:7], header)
	copy(record[7:], batchData)

	return nil
}

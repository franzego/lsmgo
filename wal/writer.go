package wal

import (
	"encoding/binary"
)

// **
//This is the full implementation of write that physically commits the raw bytes
//from the batch to the WAL on disk. The batch is ephemeral. It needs to offload
//the raw bytes to the physical disk. This understanding is important as it forms
//the basis of every other thing moving forward. The batch data is gotten after
//calling b.Repr(). Recordtype was defined in the record.go. A WAL is simply a
//flat stream of bytes in disk. Without recordType, the reader is blind. It has no idea
//where one logical entry ends and the next begins. It is the primary helper for successfully
//reading a WAL file after a crash to see which were fully flushed to disk and those with partial writes.
//This is the complete flow:
//db.Write(batch) is called
//→ wal.WriteEntry(batch.Repr()) this returns the entire batch entry as bytes
//→ check block space
//→ pad if needed
//→ fits?  writeRecord(RecordFull, data)
//→ doesn't fit? writeFragmented(data)
//→ writeRecord(RecordFirst, chunk1)
//→ writeRecord(RecordMiddle, chunk2)
//→ writeRecord(RecordLast, chunk3)
//→ sync()
// **

func (w *WAL) writeRecord(batchData []byte, rt RecordType) error {
	// build the record file that will be written to the disk (WAL)
	checksum := computeChecksum(rt, batchData)
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint16(header[4:6], uint16(len(batchData)))
	header[6] = byte(rt)
	record := make([]byte, headerSize+len(batchData))
	copy(record[0:7], header)
	copy(record[7:], batchData)
	n, err := w.file.Write(record)
	if err != nil {
		return err
	}
	// advance block offset by how much has been written to the block
	w.blockOffset += n
	return nil

}

func (w *WAL) sync() error {
	if err := w.file.Sync(); err != nil {
		return err
	}
	// Important to sync the directory too. It ensures file is visible after crash
	return w.dir.Sync()
}

func (w *WAL) WriteEntry(data []byte) error {
	remaining := blockSize - w.blockOffset

	// not enough space for even a header — pad and move to next block
	if remaining < headerSize {
		padding := make([]byte, remaining)
		w.file.Write(padding)
		w.blockOffset = 0
		remaining = blockSize
	}

	// fits in one record
	if headerSize+len(data) <= remaining {
		if err := w.writeRecord(data, RecordFull); err != nil {
			return err
		}
		return w.sync()
	}

	// needs fragmentation
	return w.writeFragmented(data)
}
func (w *WAL) writeFragmented(data []byte) error {
	first := true
	for len(data) > 0 {
		remaining := blockSize - w.blockOffset

		// pad and move to next block if not enough for a header
		if remaining < headerSize {
			padding := make([]byte, remaining)
			w.file.Write(padding)
			w.blockOffset = 0
			remaining = blockSize
		}

		// how much data can we fit in this block
		chunkSize := remaining - headerSize
		isLast := chunkSize >= len(data)
		if isLast {
			chunkSize = len(data)
		}

		var rt RecordType
		switch {
		case first && isLast:
			rt = RecordFull
		case first:
			rt = RecordFirst
		case isLast:
			rt = RecordLast
		default:
			rt = RecordMiddle
		}

		if err := w.writeRecord(data[:chunkSize], rt); err != nil {
			return err
		}

		data = data[chunkSize:]
		first = false
	}

	return w.sync()
}

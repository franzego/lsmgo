package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var ErrCorruptRecord = errors.New("wal: corrupt record")

// ReplayStats describes what happened during recovery replay.
// StopReason:
// - "eof": clean end of WAL
// - "truncated_tail": incomplete record at file tail (non-fatal)
// - "checksum_mismatch_tail": corrupted final record at file tail (non-fatal)
type ReplayStats struct {
	RecordsReplayed int
	BytesConsumed   int64
	StopReason      string
}

// replayState tracks the mutable state of the reader as it walks the WAL.
// assembling/assembled hold the in-progress logical entry built from
// First/Middle/Last physical fragments.
type replayState struct {
	offset     int64
	assembling bool
	assembled  []byte
}

// recordRead represents one physical record decoded from disk bytes.
type recordRead struct {
	start    int64
	end      int64
	rt       RecordType
	checksum uint32
	payload  []byte
}

// stopTail centralizes tail-stop behavior so replay exits consistently when
// tail corruption/truncation is considered recoverable.
func stopTail(stats ReplayStats, reason string, consumed int64) (ReplayStats, error) {
	stats.StopReason = reason
	stats.BytesConsumed = consumed
	return stats, nil
}

// readPhysicalRecord reads exactly one physical WAL record starting at st.offset.
// It also handles block padding and truncated-tail detection.
// Returns:
// - rec != nil: a valid record was read
// - stopped == true: replay should stop non-fatally with returned tail stats
// - err != nil: fatal I/O or structural error
func readPhysicalRecord(f fileLike, fileSize int64, st *replayState, header []byte) (*recordRead, ReplayStats, bool, error) {
	remainInBlock := int64(blockSize) - (st.offset % int64(blockSize))
	if remainInBlock < int64(recordHeaderSize) {
		// End-of-block padding region. If the file ends here, this is a crash-tail.
		if st.offset+remainInBlock >= fileSize {
			stats, err := stopTail(ReplayStats{}, "truncated_tail", fileSize)
			return nil, stats, true, err
		}
		st.offset += remainInBlock
	}

	recordStart := st.offset
	if recordStart+int64(recordHeaderSize) > fileSize {
		stats, err := stopTail(ReplayStats{}, "truncated_tail", recordStart)
		return nil, stats, true, err
	}

	// Header is always 7 bytes: checksum(4), payloadLen(2), recordType(1).
	if _, err := f.ReadAt(header, st.offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			stats, stopErr := stopTail(ReplayStats{}, "truncated_tail", recordStart)
			return nil, stats, true, stopErr
		}
		return nil, ReplayStats{}, false, err
	}
	st.offset += int64(recordHeaderSize)

	checksum := binary.LittleEndian.Uint32(header[0:4])
	payloadLen := int64(binary.LittleEndian.Uint16(header[4:6]))
	rt := RecordType(header[6])

	payloadEnd := st.offset + payloadLen
	// Payload crossing EOF is an incomplete tail write.
	if payloadEnd > fileSize {
		stats, err := stopTail(ReplayStats{}, "truncated_tail", recordStart)
		return nil, stats, true, err
	}

	payload := make([]byte, payloadLen)
	if _, err := f.ReadAt(payload, st.offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			stats, stopErr := stopTail(ReplayStats{}, "truncated_tail", recordStart)
			return nil, stats, true, stopErr
		}
		return nil, ReplayStats{}, false, err
	}
	st.offset = payloadEnd

	return &recordRead{
		start:    recordStart,
		end:      payloadEnd,
		rt:       rt,
		checksum: checksum,
		payload:  payload,
	}, ReplayStats{}, false, nil
}

// validateChecksum enforces corruption policy:
// - mismatch on final record at tail: non-fatal stop
// - mismatch before tail (middle-file corruption): fatal
func validateChecksum(rec *recordRead, fileSize int64) (ReplayStats, bool, error) {
	if got := computeChecksum(rec.rt, rec.payload); got == rec.checksum {
		return ReplayStats{}, false, nil
	}
	if rec.end == fileSize {
		stats, err := stopTail(ReplayStats{}, "checksum_mismatch_tail", rec.start)
		return stats, true, err
	}
	return ReplayStats{}, false, fmt.Errorf("%w: checksum mismatch at offset=%d", ErrCorruptRecord, rec.start)
}

// assembleRecord applies the WAL framing state machine.
// It returns a completed logical entry only when a Full or Last record closes
// an entry; otherwise it returns nil and replay continues.
func assembleRecord(st *replayState, rec *recordRead) (*LogEntry, error) {
	switch rec.rt {
	case RecordFull:
		entry, err := DecodeEntry(rec.payload)
		if err != nil {
			return nil, err
		}
		st.assembling = false
		st.assembled = st.assembled[:0]
		return entry, nil
	case RecordFirst:
		st.assembling = true
		st.assembled = append(st.assembled[:0], rec.payload...)
		return nil, nil
	case RecordMiddle:
		if !st.assembling {
			return nil, fmt.Errorf("%w: middle fragment without first at offset=%d", ErrCorruptRecord, rec.start)
		}
		st.assembled = append(st.assembled, rec.payload...)
		return nil, nil
	case RecordLast:
		if !st.assembling {
			return nil, fmt.Errorf("%w: last fragment without first at offset=%d", ErrCorruptRecord, rec.start)
		}
		st.assembled = append(st.assembled, rec.payload...)
		entry, err := DecodeEntry(st.assembled)
		if err != nil {
			return nil, err
		}
		st.assembling = false
		st.assembled = st.assembled[:0]
		return entry, nil
	default:
		return nil, fmt.Errorf("%w: unknown record type=%d at offset=%d", ErrCorruptRecord, rec.rt, rec.start)
	}
}

// Replay scans the current WAL file in write order and reconstructs logical
// log entries from physical records.
//
// Corruption/truncation policy:
// 1) Truncated tail (EOF in header/payload): stop non-fatally.
// 2) Tail checksum mismatch: stop non-fatally.
// 3) Middle-file checksum mismatch: fatal error.
//
// The callback is invoked once per reconstructed logical entry.
func (w *WAL) Replay(fn func(*LogEntry) error) (ReplayStats, error) {
	path := parseWALName(w.dirname, w.walNum)
	f, err := w.fs.Open(path)
	if err != nil {
		return ReplayStats{}, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return ReplayStats{}, err
	}
	fileSize := info.Size()

	stats := ReplayStats{}
	st := replayState{}
	header := make([]byte, recordHeaderSize)
	for st.offset < fileSize {
		rec, tailStats, stopped, err := readPhysicalRecord(f, fileSize, &st, header)
		if stopped {
			tailStats.RecordsReplayed = stats.RecordsReplayed
			return tailStats, err
		}
		if err != nil {
			return stats, err
		}

		tailStats, stopped, err = validateChecksum(rec, fileSize)
		if stopped {
			tailStats.RecordsReplayed = stats.RecordsReplayed
			return tailStats, err
		}
		if err != nil {
			return stats, err
		}

		entry, err := assembleRecord(&st, rec)
		if err != nil {
			return stats, err
		}
		if entry == nil {
			continue
		}
		if err := fn(entry); err != nil {
			return stats, err
		}
		stats.RecordsReplayed++
	}

	if st.assembling {
		stats.StopReason = "truncated_tail"
	} else {
		stats.StopReason = "eof"
	}
	stats.BytesConsumed = st.offset
	return stats, nil
}

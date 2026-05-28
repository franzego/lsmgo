package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type ReplayStats struct {
	RecordsReplayed int
	BytesConsumed   int64
	StopReason      string
}

type replayState struct {
	offset     int64
	assembling bool
	assembled  []byte
}

type recordRead struct {
	start    int64
	end      int64
	rt       RecordType
	checksum uint32
	payload  []byte
}

func Replay(f FileLike, fn func([]byte) error) (ReplayStats, error) {
	info, err := f.Stat()
	if err != nil {
		return ReplayStats{}, err
	}
	fileSize := info.Size()

	stats := ReplayStats{}
	st := replayState{}
	header := make([]byte, RecordHeaderSize)
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

func stopTail(stats ReplayStats, reason string, consumed int64) (ReplayStats, error) {
	stats.StopReason = reason
	stats.BytesConsumed = consumed
	return stats, nil
}

func readPhysicalRecord(f FileLike, fileSize int64, st *replayState, header []byte) (*recordRead, ReplayStats, bool, error) {
	remainInBlock := int64(BlockSize) - (st.offset % int64(BlockSize))
	if remainInBlock < int64(RecordHeaderSize) {
		if st.offset+remainInBlock >= fileSize {
			stats, err := stopTail(ReplayStats{}, "truncated_tail", fileSize)
			return nil, stats, true, err
		}
		st.offset += remainInBlock
	}

	recordStart := st.offset
	if recordStart+int64(RecordHeaderSize) > fileSize {
		stats, err := stopTail(ReplayStats{}, "truncated_tail", recordStart)
		return nil, stats, true, err
	}

	if _, err := f.ReadAt(header, st.offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			stats, stopErr := stopTail(ReplayStats{}, "truncated_tail", recordStart)
			return nil, stats, true, stopErr
		}
		return nil, ReplayStats{}, false, err
	}
	st.offset += int64(RecordHeaderSize)

	checksum := binary.LittleEndian.Uint32(header[0:4])
	payloadLen := int64(binary.LittleEndian.Uint16(header[4:6]))
	rt := RecordType(header[6])

	payloadEnd := st.offset + payloadLen
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

func validateChecksum(rec *recordRead, fileSize int64) (ReplayStats, bool, error) {
	if got := ComputeChecksum(rec.rt, rec.payload); got == rec.checksum {
		return ReplayStats{}, false, nil
	}
	if rec.end == fileSize {
		stats, err := stopTail(ReplayStats{}, "checksum_mismatch_tail", rec.start)
		return stats, true, err
	}
	return ReplayStats{}, false, fmt.Errorf("%w: checksum mismatch at offset=%d", ErrCorruptRecord, rec.start)
}

func assembleRecord(st *replayState, rec *recordRead) ([]byte, error) {
	switch rec.rt {
	case RecordFull:
		st.assembling = false
		st.assembled = st.assembled[:0]
		return rec.payload, nil
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
		entry := append([]byte(nil), st.assembled...)
		st.assembling = false
		st.assembled = st.assembled[:0]
		return entry, nil
	default:
		return nil, fmt.Errorf("%w: unknown record type=%d at offset=%d", ErrCorruptRecord, rec.rt, rec.start)
	}
}

package wal

import (
	"errors"
	"fmt"

	ilog "github.com/franzego/lsm-golang/internal/log"
)

// ReplayStats describes what happened during recovery replay.
// StopReason:
// - "eof": clean end of WAL
// - "truncated_tail": incomplete record at file tail (non-fatal)
// - "checksum_mismatch_tail": corrupted final record at file tail (non-fatal)
type ReplayStats = ilog.ReplayStats

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

	stats, err := ilog.Replay(f, func(data []byte) error {
		entry, err := DecodeEntry(data)
		if err != nil {
			return err
		}
		return fn(entry)
	})
	if errors.Is(err, ilog.ErrCorruptRecord) {
		err = fmt.Errorf("%w: %v", ErrCorruptRecord, err)
	}
	return stats, err
}

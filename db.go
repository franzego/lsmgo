package lsm

import (
	"errors"
	"sync/atomic"

	"github.com/franzego/lsm-golang/batch"
	"github.com/franzego/lsm-golang/wal"
)

type DB struct {
	seqNum  atomic.Uint64
	wal     *wal.WAL
	BatchDb batch.Batch
}

var ErrNilBatch = errors.New("lsm: nil batch")
var ErrWALNotConfigured = errors.New("lsm: wal not configured")

func Open(w *wal.WAL) *DB {
	return &DB{wal: w}
}

// OpenWithRecovery builds a DB and immediately replays the WAL.
// The apply callback is invoked for each recovered log entry in replay order.
func OpenWithRecovery(w *wal.WAL, apply func(*wal.LogEntry) error) (*DB, wal.ReplayStats, error) {
	db := Open(w)
	stats, err := db.Recover(apply)
	if err != nil {
		return nil, stats, err
	}
	return db, stats, nil
}

func (d *DB) Write(b *batch.Batch) error {
	if b == nil {
		return ErrNilBatch
	}
	if d.wal == nil {
		return ErrWALNotConfigured
	}

	seqNum := d.seqNum.Add(1)
	b.SetSeqNum(seqNum)
	if err := d.wal.WriteLogEntry(b.Repr()); err != nil {
		return err
	}
	b.Applied.Store(true)
	return nil
}

// Recover replays WAL entries and restores DB sequence state.
// It is safe to call with apply == nil when only sequence restoration is needed.
func (d *DB) Recover(apply func(*wal.LogEntry) error) (wal.ReplayStats, error) {
	if d.wal == nil {
		return wal.ReplayStats{}, ErrWALNotConfigured
	}

	var maxSeq uint64
	stats, err := d.wal.Replay(func(e *wal.LogEntry) error {
		if e.SeqNum > maxSeq {
			maxSeq = e.SeqNum
		}
		if apply != nil {
			return apply(e)
		}
		return nil
	})
	if err != nil {
		return stats, err
	}
	d.seqNum.Store(maxSeq)
	return stats, nil
}

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

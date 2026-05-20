package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/franzego/lsm-golang/batch"
	"github.com/franzego/lsm-golang/memtable"
	"github.com/franzego/lsm-golang/wal"
)

// DB serializes the write path so sequence assignment, WAL durability,
// and memtable visibility share one commit order seen by all readers.
type DB struct {
	mu      sync.Mutex
	seqNum  atomic.Uint64
	wal     *wal.WAL
	mem     *memtable.MemTable
	BatchDb batch.Batch
}

var (
	ErrNilBatch         = errors.New("lsm: nil batch")
	ErrWALNotConfigured = errors.New("lsm: wal not configured")
	ErrEmptyBatch       = errors.New("lsm: empty batch")
	ErrCorruptBatch     = errors.New("lsm: corrupt batch")
)

func Open(w *wal.WAL) *DB {
	return &DB{
		wal: w,
		mem: memtable.NewMemtable(),
	}
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
	// A committed batch reserves one contiguous sequence range, is durable in WAL,
	// then becomes visible in memtable with per-op sequence derived from that range.
	d.mu.Lock()
	defer d.mu.Unlock()
	count32 := b.Count()
	if count32 == 0 {
		return ErrEmptyBatch
	}
	count := uint64(count32)
	last := d.seqNum.Add(count)
	// WAL header sequence is the first op sequence in this batch's reserved range.
	first := last - count + 1
	b.SetSeqNum(first)
	if err := d.wal.WriteLogEntry(b.Repr()); err != nil {
		return err
	}
	if err := validateAndIterateBatchOps(b.Repr(), count32, first, func(opType byte, key, value []byte, seq uint64) error {
		switch opType {
		case batch.OpTypePut:
			d.mem.ApplyPut(key, value, seq)
			return nil
		case batch.OpTypeDelete:
			d.mem.ApplyDelete(key, seq)
			return nil
		default:
			return ErrCorruptBatch
		}
	}); err != nil {
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
		if e.Count == 0 {
			return ErrCorruptBatch
		}
		// Recovery must restore the end of each reserved range so future writes stay monotonic.
		lastSeq := e.SeqNum + uint64(e.Count) - 1
		if lastSeq > maxSeq {
			maxSeq = lastSeq
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

func validateAndIterateBatchOps(data []byte, count uint32, firstSeq uint64, fn func(opType byte, key, value []byte, seq uint64) error) error {
	const batchHeaderLen = 12
	const opHeaderLen = 9 // opType(1) + keyLen(4) + valueLen(4)

	// Strict bounds checks make truncated/corrupt payloads fail closed before memtable mutation.
	if len(data) < batchHeaderLen {
		return ErrCorruptBatch
	}

	off := batchHeaderLen
	for i := uint32(0); i < count; i++ {
		if off+opHeaderLen > len(data) {
			return ErrCorruptBatch
		}
		opType := data[off]
		keyLen := int(binary.LittleEndian.Uint32(data[off+1 : off+5]))
		valLen := int(binary.LittleEndian.Uint32(data[off+5 : off+9]))
		off += opHeaderLen

		if off+keyLen+valLen > len(data) {
			return ErrCorruptBatch
		}
		key := data[off : off+keyLen]
		off += keyLen
		value := data[off : off+valLen]
		off += valLen

		seq := firstSeq + uint64(i)
		if err := fn(opType, key, value, seq); err != nil {
			return fmt.Errorf("batch op idx=%d: %w", i, err)
		}
	}

	return nil
}

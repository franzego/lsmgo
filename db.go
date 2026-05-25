package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/franzego/lsm-golang/batch"
	"github.com/franzego/lsm-golang/memtable"
	"github.com/franzego/lsm-golang/sstable"
	"github.com/franzego/lsm-golang/wal"
)

// DB serializes the write path so sequence assignment, WAL durability,
// and memtable visibility share one commit order seen by all readers.
type DB struct {
	mu         sync.RWMutex
	seqNum     atomic.Uint64
	wal        *wal.WAL
	mem        *memtable.MemTable
	immutables []*memtable.MemTable
	opts       options

	nextSSTableNum uint64
	nextFlushIdx   int
}

type options struct {
	memTableThresholdBytes int
	sstableDir             string
}

type Option func(*options)

func WithMemTableThresholdBytes(n int) Option {
	return func(o *options) {
		o.memTableThresholdBytes = n
	}
}

func WithSSTableDir(dir string) Option {
	return func(o *options) {
		o.sstableDir = dir
	}
}

func (o *options) ensureDefaults() {
	if o.memTableThresholdBytes <= 0 {
		o.memTableThresholdBytes = memtable.DefaultThresholdBytes
	}
	if o.sstableDir == "" {
		o.sstableDir = "sstable"
	}
}

var (
	ErrNilBatch         = errors.New("lsm: nil batch")
	ErrWALNotConfigured = errors.New("lsm: wal not configured")
	ErrEmptyBatch       = errors.New("lsm: empty batch")
	ErrCorruptBatch     = errors.New("lsm: corrupt batch")
)

func Open(w *wal.WAL, opts ...Option) *DB {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.ensureDefaults()
	db := &DB{
		wal:            w,
		mem:            memtable.NewMemtable(),
		opts:           cfg,
		nextSSTableNum: 1,
	}
	return db
}

// OpenWithRecovery builds a DB and immediately replays the WAL.
// The apply callback is invoked for each recovered log entry in replay order.
func OpenWithRecovery(w *wal.WAL, apply func(*wal.LogEntry) error, opts ...Option) (*DB, wal.ReplayStats, error) {
	db := Open(w, opts...)
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
	first := (last - count) + 1
	b.SetSeqNum(first)
	// Strict parse/validation runs before WAL append so corruption never becomes durable.
	ops, err := parseBatchOps(b.Repr(), count32)
	if err != nil {
		return err
	}
	if err := d.wal.WriteLogEntry(b.Repr()); err != nil {
		return err
	}
	for i, op := range ops {
		seq := first + uint64(i)
		switch op.opType {
		case batch.OpTypePut:
			d.mem.ApplyPut(op.key, op.value, seq)
		case batch.OpTypeDelete:
			d.mem.ApplyDelete(op.key, seq)
		default:
			return ErrCorruptBatch
		}
	}

	d.rotateMemtableIfNeeded()
	b.Applied.Store(true)
	return nil
}

func (d *DB) Get(key []byte) ([]byte, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if value, found, deleted := valueFromMemtable(d.mem, key); found {
		if deleted {
			return nil, false
		}
		return value, true
	}
	for i := len(d.immutables) - 1; i >= 0; i-- {
		value, found, deleted := valueFromMemtable(d.immutables[i], key)
		if !found {
			continue
		}
		if deleted {
			return nil, false
		}
		return value, true
	}
	return nil, false
}

func valueFromMemtable(m *memtable.MemTable, key []byte) ([]byte, bool, bool) {
	ent, ok := m.GetLatest(key)
	if !ok {
		return nil, false, false
	}
	if ent.Key.Kind == memtable.KindTombstone {
		return nil, true, true
	}
	return append([]byte(nil), ent.Value...), true, false
}

func (d *DB) rotateMemtableIfNeeded() bool {
	if d.mem.Len() == 0 || d.mem.ApproxBytes() < d.opts.memTableThresholdBytes {
		return false
	}
	d.immutables = append(d.immutables, d.mem)
	d.mem = memtable.NewMemtable()
	return true
}

func (d *DB) FlushOneImmutable() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.nextFlushIdx >= len(d.immutables) {
		return nil
	}
	mem := d.immutables[d.nextFlushIdx]
	path := filepath.Join(d.opts.sstableDir, fmt.Sprintf("%06d.sst", d.nextSSTableNum))
	if err := sstable.Write(path, mem.Entries()); err != nil {
		return err
	}
	d.nextFlushIdx++
	d.nextSSTableNum++
	return nil
}

func (d *DB) Close() error {
	return nil
}

// Recover replays WAL entries and restores DB sequence state.
// It is safe to call with apply == nil when only sequence restoration is needed.
func (d *DB) Recover(apply func(*wal.LogEntry) error) (wal.ReplayStats, error) {
	if d.wal == nil {
		return wal.ReplayStats{}, ErrWALNotConfigured
	}
	d.mu.Lock()
	defer d.mu.Unlock()

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

type parsedOp struct {
	opType byte
	key    []byte
	value  []byte
}

func parseBatchOps(data []byte, count uint32) ([]parsedOp, error) {
	const batchHeaderLen = 12
	const opHeaderLen = 9 // opType(1) + keyLen(4) + valueLen(4)
	if len(data) < batchHeaderLen {
		return nil, ErrCorruptBatch
	}

	ops := make([]parsedOp, 0, count)
	off := batchHeaderLen
	for i := uint32(0); i < count; i++ {
		if off+opHeaderLen > len(data) {
			return nil, ErrCorruptBatch
		}
		opType := data[off]
		if opType != batch.OpTypePut && opType != batch.OpTypeDelete {
			return nil, ErrCorruptBatch
		}
		keyLen := int(binary.LittleEndian.Uint32(data[off+1 : off+5]))
		valLen := int(binary.LittleEndian.Uint32(data[off+5 : off+9]))
		off += opHeaderLen
		if off+keyLen+valLen > len(data) {
			return nil, ErrCorruptBatch
		}
		key := data[off : off+keyLen]
		off += keyLen
		value := data[off : off+valLen]
		off += valLen
		ops = append(ops, parsedOp{
			opType: opType,
			key:    key,
			value:  value,
		})
	}

	return ops, nil
}

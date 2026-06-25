package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/franzego/lsm-golang/batch"
	"github.com/franzego/lsm-golang/manifest"
	"github.com/franzego/lsm-golang/memtable"
	"github.com/franzego/lsm-golang/sstable"
	"github.com/franzego/lsm-golang/wal"
)

// DB serializes the write path so sequence assignment, WAL durability,
// and memtable visibility share one commit order seen by all readers.
type DB struct {
	mu          sync.RWMutex
	seqNum      atomic.Uint64
	wal         walLog
	mem         *memtable.MemTable
	immutables  []*memtable.MemTable
	sstables    []string
	manifest    *manifest.Manifest
	manifestErr error
	opts        options

	nextSSTableNum uint64
}

type walLog interface {
	WriteLogEntry([]byte) error
	Replay(func(*wal.LogEntry) error) (wal.ReplayStats, error)
	Close() error
}

type options struct {
	memTableThresholdBytes int
	sstableDir             string
	sstableDirConfigured   bool
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
		o.sstableDirConfigured = true
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

func Open(path string, opts ...Option) (*DB, error) {
	db, _, err := OpenWithRecovery(path, nil, opts...)
	return db, err
}

// OpenWithRecovery opens a DB rooted at path and replays the current WAL before
// returning. The optional apply callback observes recovered WAL entries after
// they have been applied to the memtable.
func OpenWithRecovery(path string, apply func(*wal.LogEntry) error, opts ...Option) (*DB, wal.ReplayStats, error) {
	cfg := buildOptions(path, true, opts...)

	w, err := wal.Open(filepath.Join(path, "wal"), 1)
	if err != nil {
		return nil, wal.ReplayStats{}, err
	}
	db, err := newDB(w, cfg)
	if err != nil {
		_ = w.Close()
		return nil, wal.ReplayStats{}, err
	}
	stats, err := db.Recover(apply)
	if err != nil {
		_ = db.Close()
		return nil, stats, err
	}
	return db, stats, nil
}

func openWithWAL(w walLog, opts ...Option) *DB {
	cfg := buildOptions("", false, opts...)
	db, _ := newDB(w, cfg)
	return db
}

func buildOptions(rootDir string, defaultSSTableDir bool, opts ...Option) options {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}
	if defaultSSTableDir && !cfg.sstableDirConfigured {
		cfg.sstableDir = filepath.Join(rootDir, "sstable")
		cfg.sstableDirConfigured = true
	}
	cfg.ensureDefaults()
	return cfg
}

func newDB(w walLog, cfg options) (*DB, error) {
	db := &DB{
		wal:            w,
		mem:            memtable.NewMemtable(),
		opts:           cfg,
		nextSSTableNum: 1,
	}
	if cfg.sstableDirConfigured {
		m, state, err := manifest.Open(cfg.sstableDir)
		if err != nil {
			db.manifestErr = err
			return db, err
		}
		db.manifest = m
		db.nextSSTableNum = state.NextFileNum
		for _, table := range state.SSTables {
			db.sstables = append(db.sstables, table.Path)
		}
	}
	return db, nil
}

func (d *DB) Put(key, value []byte) error {
	var b batch.Batch
	if err := b.Put(key, value); err != nil {
		return err
	}
	return d.Write(&b)
}

func (d *DB) Delete(key []byte) error {
	var b batch.Batch
	if err := b.Delete(key); err != nil {
		return err
	}
	return d.Write(&b)
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
	for i := len(d.sstables) - 1; i >= 0; i-- {
		value, found, deleted, err := sstable.Get(d.sstables[i], key)
		if err != nil || !found {
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

	if len(d.immutables) == 0 {
		return nil
	}
	if err := d.ensureManifestReadyLocked(); err != nil {
		return err
	}
	mem := d.immutables[0]
	fileNum := d.nextSSTableNum
	path := filepath.Join(d.opts.sstableDir, fmt.Sprintf("%06d.sst", fileNum))
	if err := sstable.Write(path, mem.Entries()); err != nil {
		return err
	}
	nextFileNum := fileNum + 1
	if d.manifest != nil {
		err := d.manifest.Append(manifest.Edit{
			AddSSTables: []manifest.SSTable{{
				FileNum: fileNum,
				Path:    path,
			}},
			NextFileNum: &nextFileNum,
		})
		if err != nil {
			return err
		}
	}
	d.immutables = d.immutables[1:]
	d.sstables = append(d.sstables, path)
	d.nextSSTableNum = nextFileNum
	return nil
}

func (d *DB) ensureManifestReadyLocked() error {
	if !d.opts.sstableDirConfigured || d.manifestErr == nil {
		return d.manifestErr
	}
	m, state, err := manifest.Open(d.opts.sstableDir)
	if err != nil {
		d.manifestErr = err
		return err
	}
	d.manifest = m
	d.manifestErr = nil
	d.sstables = d.sstables[:0]
	d.nextSSTableNum = state.NextFileNum
	for _, table := range state.SSTables {
		d.sstables = append(d.sstables, table.Path)
	}
	return nil
}

func (d *DB) Close() error {
	var err error
	if d.manifest != nil {
		err = d.manifest.Close()
	}
	if d.wal != nil {
		if walErr := d.wal.Close(); err == nil {
			err = walErr
		}
	}
	return err
}

// Recover replays WAL entries and restores DB sequence state.
// It is safe to call with apply == nil when no external recovery observer is needed.
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
		ops, err := parseBatchOps(e.Data, e.Count)
		if err != nil {
			return err
		}
		for i, op := range ops {
			seq := e.SeqNum + uint64(i)
			switch op.opType {
			case batch.OpTypePut:
				d.mem.ApplyPut(op.key, op.value, seq)
			case batch.OpTypeDelete:
				d.mem.ApplyDelete(op.key, seq)
			default:
				return ErrCorruptBatch
			}
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

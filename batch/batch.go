package batch

import (
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
)

const (
	defaultBatchInitialSize          = 1 << 10 // 1 KB
	defaultBatchMaxRetainedSize      = 1 << 20 // 1 MB
	OpTypePut                   byte = 1
	OpTypeDelete                byte = 2
)

const headerLength = 12 // 8 for seqNum, 4 for count
var ErrEmptyKey = errors.New("batch: empty key")

func (b *Batch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	if b.data == nil || len(b.data) < headerLength {
		b.init(headerLength)
	}

	recordLen := int(OpTypePut) + 8 + len(key) + len(value) // keyLen(4) + valueLen(4) + key + value
	offset := len(b.data)
	b.grow(offset + recordLen)

	b.data[offset] = OpTypePut
	binary.LittleEndian.PutUint32(b.data[offset+1:offset+5], uint32(len(key)))
	binary.LittleEndian.PutUint32(b.data[offset+5:offset+9], uint32(len(value)))
	copy(b.data[offset+9:offset+9+len(key)], key)
	copy(b.data[offset+9+len(key):], value)

	c := b.count()
	c++
	b.SetCount(c)
	return nil
}

// count provides a single source of truth for getting the number of items[key value pairs]/ operations that
// have been done in the batch.
func (b *Batch) count() uint32 {
	if len(b.data) < headerLength {
		return 0
	}
	return binary.LittleEndian.Uint32(b.data[8:12])
}
func (b *Batch) SetCount(count uint32) {
	b.ensureHeader()
	binary.LittleEndian.PutUint32(b.data[8:12], count)
}

func (b *Batch) SeqNum() uint64 {
	if len(b.data) < headerLength {
		return 0
	}
	return binary.LittleEndian.Uint64(b.data[0:8])
}

// SetSeqNum is what the db.Write() will call at the commit time - right before
// handing over to write to the WAL.
func (b *Batch) SetSeqNum(seq uint64) {
	b.ensureHeader()
	binary.LittleEndian.PutUint64(b.data[0:8], seq)
}

// Repr is to return the raw bytes that had been saved to the batch. This is to
// be called when the WAL needs the raw bytes.
func (b *Batch) Repr() []byte {
	return b.data
}

// BatchFromRepr on the recovery side ensures the WAL reader reassembles the raw
// bytes from physical records and the batch layer reconstructs from those bytes.
// Let it be here for now.
func BatchFromRepr(data []byte) *Batch {
	b := batchPool.Get().(*Batch)
	if len(data) < headerLength {
		b.init(headerLength)
		copy(b.data, data)
		return b
	}
	b.data = append([]byte(nil), data...)
	return b
}

// batchPool helps reuse batches after they have been closed or freed. This
// prevents GC pressure of having to keep creating batches all the time.
var batchPool = sync.Pool{
	New: func() interface{} {
		return &Batch{}
	},
}

func (b *Batch) Reset() {
	b.batchInternal = batchInternal{
		data:       b.data,
		committing: false,
		opts:       b.opts,
	}
	b.Applied.Store(false)
	if b.data != nil {
		if cap(b.data) > defaultBatchMaxRetainedSize { // no need for reuse. Just get rid of it with the GC.
			b.data = nil
		} else {
			b.data = b.data[:12] //the idea is to have the seqNum occupy 8 bytes and the count occupy 4 bytes
			clear(b.data)
		}
	}
}

type batchOptions struct {
	initialSizeBytes  int // this sets the initial size of the batch. It defaults to 1kb
	maxReuseSizeBytes int // this sets the max size of the batch that can be reused. Any batch that exceeds this, is handled by the GC.
}
type OptionBatch func(*batchOptions)

func WithInitializeSizeBytes(s int) OptionBatch {
	return func(bopt *batchOptions) {
		bopt.initialSizeBytes = s
	}
}
func WithMaxReuseSizeBytes(s int) OptionBatch {
	return func(bopt *batchOptions) {
		bopt.maxReuseSizeBytes = s
	}
}
func (bo *batchOptions) ensureDefaults() {
	if bo.initialSizeBytes <= 0 {
		bo.initialSizeBytes = defaultBatchInitialSize
	}
	if bo.maxReuseSizeBytes <= 0 {
		bo.maxReuseSizeBytes = defaultBatchMaxRetainedSize
	}
}
func newBatch(opts ...OptionBatch) *Batch {
	b := batchPool.Get().(*Batch)
	for _, opt := range opts {
		opt(&b.opts)
	}
	b.opts.ensureDefaults()
	return b
}
func newBatchWithSize(size int, opts ...OptionBatch) *Batch {
	b := newBatch(opts...)
	if cap(b.data) < size {
		n := b.opts.initialSizeBytes
		for n < size {
			n *= 2
		}
		b.data = make([]byte, size, n)
	} else {
		b.data = b.data[:size]
	}
	return b
}
func (b *Batch) init(size int) {
	b.opts.ensureDefaults()
	n := b.opts.initialSizeBytes
	for n < size {
		n *= 2
	}
	if cap(b.data) < n {
		b.data = make([]byte, size, n)
	} else {
		b.data = b.data[:size]
	}
}

func (b *Batch) grow(size int) {
	if cap(b.data) >= size {
		b.data = b.data[:size]
		return
	}

	capacity := cap(b.data)
	if capacity == 0 {
		capacity = b.opts.initialSizeBytes
		if capacity <= 0 {
			capacity = defaultBatchInitialSize
		}
	}
	for capacity < size {
		capacity *= 2
	}

	next := make([]byte, size, capacity)
	copy(next, b.data)
	b.data = next
}

func (b *Batch) ensureHeader() {
	if len(b.data) < headerLength {
		b.init(headerLength)
	}
}

type batchInternal struct {
	// batchSeqNum uint64 // this is a sequence number for every op that is carried out.
	// count      uint64 // number of items in a batch
	data       []byte //this will contain the seqNum and the count for the operations (set, deletee)
	committing bool   // set to true when a batch starts committing.
	opts       batchOptions

	// db *Db
	// memTable size uint64
}

type Batch struct {
	batchInternal
	Applied atomic.Bool
}

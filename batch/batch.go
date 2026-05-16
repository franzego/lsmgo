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

var headerLength int = 12 // 8 for seqNum, 4 for count
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

	b.Count++
	binary.LittleEndian.PutUint32(b.data[8:12], uint32(b.Count))
	return nil
}

// delete
// serialization

// batchPool helps reuse batches after they have been closed or freed. This prevents GC pressure of having to keep
// creating batches all the time.
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
		b.data = make([]byte, size)
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

type batchInternal struct {
	// batchSeqNum uint64 // this is a sequence number for every op that is carried out. it
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
	Count   uint64
}

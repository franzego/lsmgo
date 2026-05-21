package memtable

import (
	"sync"

	"github.com/huandu/skiplist"
)

type Kind byte

const (
	KindPut       Kind = 1
	KindTombstone Kind = 2

	DefaultThresholdBytes = 64 << 20
	approxEntryOverhead   = 32
)

type InternalKey struct {
	// Memtable keys keep versions by (userKey, seq, kind), enabling coexistence
	// and deterministic shadowing of older entries.
	UserKey []byte
	SeqNum  uint64
	Kind    Kind
}

type Entry struct {
	Key   InternalKey
	Value []byte
}

type MemTable struct {
	mu          sync.RWMutex
	list        *skiplist.SkipList
	approxBytes int
}

func NewMemtable() *MemTable {
	return &MemTable{
		list: newSkipList(),
	}
}

func (m *MemTable) ApplyPut(key, value []byte, seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Memtable owns immutable bytes; callers may reuse or mutate their buffers.
	ownedKey := append([]byte(nil), key...)
	ownedValue := append([]byte(nil), value...)
	m.list.Set(InternalKey{
		UserKey: ownedKey,
		SeqNum:  seq,
		Kind:    KindPut,
	}, Entry{
		Key: InternalKey{
			UserKey: ownedKey,
			SeqNum:  seq,
			Kind:    KindPut,
		},
		Value: ownedValue,
	})
	m.approxBytes += len(ownedKey) + len(ownedValue) + approxEntryOverhead
}

func (m *MemTable) ApplyDelete(key []byte, seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deletes are tombstone inserts so compaction can retire older versions later.
	// Copying key bytes preserves ownership across caller buffer reuse.
	ownedKey := append([]byte(nil), key...)
	m.list.Set(InternalKey{
		UserKey: ownedKey,
		SeqNum:  seq,
		Kind:    KindTombstone,
	}, Entry{
		Key: InternalKey{
			UserKey: ownedKey,
			SeqNum:  seq,
			Kind:    KindTombstone,
		},
	})
	m.approxBytes += len(ownedKey) + approxEntryOverhead
}

func (m *MemTable) GetLatest(key []byte) (Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	probe := InternalKey{
		UserKey: append([]byte(nil), key...),
		// Probe with max sequence to land on newest version under current key ordering.
		SeqNum: ^uint64(0),
		Kind:   KindPut,
	}
	e := m.list.Find(probe)
	if e == nil {
		return Entry{}, false
	}
	ent := e.Value.(Entry)
	// Find returns first >= probe; still verify same user key to reject neighbor keys.
	if compareUserKeys(ent.Key.UserKey, key) != 0 {
		return Entry{}, false
	}
	return ent, true
}

func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.Len()
}

func (m *MemTable) ApproxBytes() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.approxBytes
}

func (m *MemTable) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.list.Init()
	m.approxBytes = 0
}

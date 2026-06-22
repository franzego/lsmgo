package memtable

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestGetLatestReturnsHighestSeqForSameKey(t *testing.T) {
	m := NewMemtable()
	m.ApplyPut([]byte("david"), []byte("lover"), 1)
	m.ApplyPut([]byte("david"), []byte("builder"), 2)

	got, ok := m.GetLatest([]byte("david"))
	if !ok {
		t.Fatalf("expected latest entry for key")
	}
	if got.Key.SeqNum != 2 {
		t.Fatalf("expected latest seq=2, got %d", got.Key.SeqNum)
	}
	if string(got.Value) != "builder" {
		t.Fatalf("expected latest value=builder, got %q", string(got.Value))
	}
}

func TestGetLatestReturnsTombstone(t *testing.T) {
	m := NewMemtable()
	m.ApplyPut([]byte("k"), []byte("v"), 7)
	m.ApplyDelete([]byte("k"), 8)

	got, ok := m.GetLatest([]byte("k"))
	if !ok {
		t.Fatalf("expected latest entry for key")
	}
	if got.Key.Kind != KindTombstone {
		t.Fatalf("expected tombstone kind, got %d", got.Key.Kind)
	}
}

func TestApproxBytesAndReset(t *testing.T) {
	m := NewMemtable()
	m.ApplyPut([]byte("k"), []byte("value"), 1)
	if got := m.ApproxBytes(); got <= 0 {
		t.Fatalf("expected approximate bytes to increase, got %d", got)
	}

	m.Reset()
	if got := m.Len(); got != 0 {
		t.Fatalf("expected reset memtable len=0, got %d", got)
	}
	if got := m.ApproxBytes(); got != 0 {
		t.Fatalf("expected reset approximate bytes=0, got %d", got)
	}
}

func TestEntriesReturnsSortedCopies(t *testing.T) {
	m := NewMemtable()
	m.ApplyPut([]byte("b"), []byte("two"), 2)
	m.ApplyPut([]byte("a"), []byte("three"), 3)
	m.ApplyPut([]byte("a"), []byte("one"), 1)

	entries := m.Entries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	want := []struct {
		key string
		seq uint64
		val string
	}{
		{"a", 3, "three"},
		{"a", 1, "one"},
		{"b", 2, "two"},
	}
	for i := range want {
		if string(entries[i].Key.UserKey) != want[i].key || entries[i].Key.SeqNum != want[i].seq || string(entries[i].Value) != want[i].val {
			t.Fatalf("entry %d = (%q,%d,%q), want (%q,%d,%q)",
				i,
				entries[i].Key.UserKey,
				entries[i].Key.SeqNum,
				entries[i].Value,
				want[i].key,
				want[i].seq,
				want[i].val)
		}
	}

	entries[0].Key.UserKey[0] = 'z'
	entries[0].Value[0] = 'Z'
	again := m.Entries()
	if string(again[0].Key.UserKey) != "a" || string(again[0].Value) != "three" {
		t.Fatalf("entries snapshot mutated memtable storage: got %q=%q", again[0].Key.UserKey, again[0].Value)
	}
}

func benchmarkMemtableKey(i uint64) []byte {
	key := make([]byte, 16)
	copy(key, "bench-key")
	binary.BigEndian.PutUint64(key[8:], i)
	return key
}

func benchmarkMemtableValue() []byte {
	return []byte("bench-value-000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
}

func BenchmarkMemTableApplyPutSameKeyVersions(b *testing.B) {
	m := NewMemtable()
	key := []byte("bench-key-000001")
	value := benchmarkMemtableValue()

	b.ReportAllocs()
	b.SetBytes(int64(len(key) + len(value)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.ApplyPut(key, value, uint64(i+1))
	}
}

func BenchmarkMemTableApplyPutUniqueKeys(b *testing.B) {
	value := benchmarkMemtableValue()

	for _, prefill := range []int{0, 1_000, 10_000} {
		b.Run(fmt.Sprintf("prefill_%d", prefill), func(b *testing.B) {
			m := NewMemtable()
			for i := 0; i < prefill; i++ {
				m.ApplyPut(benchmarkMemtableKey(uint64(i)), value, uint64(i+1))
			}

			b.ReportAllocs()
			b.SetBytes(int64(16 + len(value)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				seq := uint64(prefill + i + 1)
				m.ApplyPut(benchmarkMemtableKey(seq), value, seq)
			}
		})
	}
}

func BenchmarkMemTableGetLatest(b *testing.B) {
	value := benchmarkMemtableValue()

	for _, size := range []int{1, 1_000, 10_000} {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			m := NewMemtable()
			keys := make([][]byte, size)
			for i := 0; i < size; i++ {
				key := benchmarkMemtableKey(uint64(i))
				keys[i] = key
				m.ApplyPut(key, value, uint64(i+1))
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := keys[i%len(keys)]
				if _, ok := m.GetLatest(key); !ok {
					b.Fatalf("missing key %q", key)
				}
			}
		})
	}
}

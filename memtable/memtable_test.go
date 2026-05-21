package memtable

import "testing"

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

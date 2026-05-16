package batch

import (
	"encoding/binary"
	"testing"
)

func TestSetSeqNumOnEmptyBatchInitializesHeader(t *testing.T) {
	b := newBatch()

	b.SetSeqNum(42)

	if got := b.SeqNum(); got != 42 {
		t.Fatalf("SeqNum()=%d, want 42", got)
	}
	if len(b.Repr()) < headerLength {
		t.Fatalf("repr len=%d, want at least %d", len(b.Repr()), headerLength)
	}
}

func TestSetCountOnEmptyBatchInitializesHeader(t *testing.T) {
	b := newBatch()

	b.SetCount(7)

	if got := b.count(); got != 7 {
		t.Fatalf("count()=%d, want 7", got)
	}
	if len(b.Repr()) < headerLength {
		t.Fatalf("repr len=%d, want at least %d", len(b.Repr()), headerLength)
	}
}

func TestBatchFromReprCopiesInput(t *testing.T) {
	src := make([]byte, headerLength+4)
	binary.LittleEndian.PutUint64(src[0:8], 99)
	binary.LittleEndian.PutUint32(src[8:12], 3)
	copy(src[12:], []byte{1, 2, 3, 4})

	b := BatchFromRepr(src)
	src[0] = 0
	src[12] = 9

	if got := b.SeqNum(); got != 99 {
		t.Fatalf("SeqNum()=%d, want 99", got)
	}
	if got := b.Repr()[12]; got != 1 {
		t.Fatalf("repr[12]=%d, want 1", got)
	}
}

func TestPutEncodesRecordAndUpdatesCount(t *testing.T) {
	b := newBatch()
	key := []byte("k1")
	value := []byte("v1")

	if err := b.Put(key, value); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	if got := b.count(); got != 1 {
		t.Fatalf("count()=%d, want 1", got)
	}

	repr := b.Repr()
	offset := headerLength
	if got := repr[offset]; got != OpTypePut {
		t.Fatalf("opType=%d, want %d", got, OpTypePut)
	}
	if got := binary.LittleEndian.Uint32(repr[offset+1 : offset+5]); got != uint32(len(key)) {
		t.Fatalf("keyLen=%d, want %d", got, len(key))
	}
	if got := binary.LittleEndian.Uint32(repr[offset+5 : offset+9]); got != uint32(len(value)) {
		t.Fatalf("valueLen=%d, want %d", got, len(value))
	}
	gotKey := repr[offset+9 : offset+9+len(key)]
	if string(gotKey) != string(key) {
		t.Fatalf("key=%q, want %q", gotKey, key)
	}
	gotValue := repr[offset+9+len(key) : offset+9+len(key)+len(value)]
	if string(gotValue) != string(value) {
		t.Fatalf("value=%q, want %q", gotValue, value)
	}
}

func TestGrowExceedingCapacityResizesAndPreservesData(t *testing.T) {
	b := newBatchWithSize(headerLength, WithInitializeSizeBytes(16))
	copy(b.data, []byte("abcdefghijkl")) // 12 bytes

	oldCap := cap(b.data)
	target := oldCap + 64 // force growth beyond current capacity
	b.grow(target)

	if len(b.data) != target {
		t.Fatalf("len=%d, want %d", len(b.data), target)
	}
	if cap(b.data) < target {
		t.Fatalf("cap=%d, want at least %d", cap(b.data), target)
	}
	if cap(b.data) <= oldCap {
		t.Fatalf("cap did not grow: old=%d new=%d", oldCap, cap(b.data))
	}
	if got := string(b.data[:headerLength]); got != "abcdefghijkl" {
		t.Fatalf("data prefix changed: got %q", got)
	}
}

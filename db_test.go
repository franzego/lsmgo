package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/franzego/lsm-golang/batch"
	"github.com/franzego/lsm-golang/wal"
)

func TestDBWriteAssignsSeqAndMarksApplied(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() {
		_ = w.Close()
	})

	db := Open(w)
	var b batch.Batch
	if err := b.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put batch: %v", err)
	}

	if err := db.Write(&b); err != nil {
		t.Fatalf("db write: %v", err)
	}
	if b.SeqNum() != 1 {
		t.Fatalf("expected seqnum=1, got %d", b.SeqNum())
	}
	if !b.Applied.Load() {
		t.Fatalf("expected batch to be marked applied")
	}
	if got := db.mem.Len(); got != 1 {
		t.Fatalf("expected memtable len=1, got %d", got)
	}
}

func TestDBWriteNilBatch(t *testing.T) {
	db := Open(nil)
	if err := db.Write(nil); !errors.Is(err, ErrNilBatch) {
		t.Fatalf("expected ErrNilBatch, got %v", err)
	}
}

func TestDBWriteMultiOpBatchUsesRangeStartSeq(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	db := Open(w)

	var first batch.Batch
	if err := first.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put first: %v", err)
	}
	if err := first.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put first: %v", err)
	}
	if err := first.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("put first: %v", err)
	}
	if err := db.Write(&first); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if got := first.SeqNum(); got != 1 {
		t.Fatalf("expected first batch start seq=1, got %d", got)
	}

	var second batch.Batch
	if err := second.Put([]byte("d"), []byte("4")); err != nil {
		t.Fatalf("put second: %v", err)
	}
	if err := second.Put([]byte("e"), []byte("5")); err != nil {
		t.Fatalf("put second: %v", err)
	}
	if err := db.Write(&second); err != nil {
		t.Fatalf("write second: %v", err)
	}
	if got := second.SeqNum(); got != 4 {
		t.Fatalf("expected second batch start seq=4, got %d", got)
	}
	if got := db.mem.Len(); got != 5 {
		t.Fatalf("expected memtable len=5, got %d", got)
	}
}

func TestDBWriteRejectsCorruptBatchPayload(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	db := Open(w)

	var valid batch.Batch
	if err := valid.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put valid: %v", err)
	}
	corrupt := batch.BatchFromRepr(valid.Repr()[:len(valid.Repr())-1])
	t.Cleanup(corrupt.Reset)

	err = db.Write(corrupt)
	if !errors.Is(err, ErrCorruptBatch) {
		t.Fatalf("expected ErrCorruptBatch, got %v", err)
	}
}

func TestDBWriteRejectsInvalidOpTypeBeforeWALPersist(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := wal.Open(walDir, 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	db := Open(w)

	var valid batch.Batch
	if err := valid.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put valid: %v", err)
	}

	raw := append([]byte(nil), valid.Repr()...)
	raw[12] = 99 // mutate op type byte
	invalid := batch.BatchFromRepr(raw)
	t.Cleanup(invalid.Reset)

	err = db.Write(invalid)
	if !errors.Is(err, ErrCorruptBatch) {
		t.Fatalf("expected ErrCorruptBatch, got %v", err)
	}
	if invalid.Applied.Load() {
		t.Fatalf("invalid batch should not be marked applied")
	}

	stats, err := w.Replay(func(e *wal.LogEntry) error { return nil })
	if err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	if stats.RecordsReplayed != 0 {
		t.Fatalf("expected no persisted records, got %d", stats.RecordsReplayed)
	}
}

func TestDBWriteWALFailureDoesNotApplyMemtableOrMarkApplied(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	db := Open(w)
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	var b batch.Batch
	if err := b.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put batch: %v", err)
	}
	err = db.Write(&b)
	if !errors.Is(err, wal.ErrWALClosed) {
		t.Fatalf("expected wal.ErrWALClosed, got %v", err)
	}
	if b.Applied.Load() {
		t.Fatalf("batch should not be marked applied on WAL failure")
	}
	if got := db.mem.Len(); got != 0 {
		t.Fatalf("expected memtable len=0 on WAL failure, got %d", got)
	}
}

func makeBatchRepr(t *testing.T, key, val string) []byte {
	t.Helper()
	var b batch.Batch
	if err := b.Put([]byte(key), []byte(val)); err != nil {
		t.Fatalf("batch put: %v", err)
	}
	return append([]byte(nil), b.Repr()...)
}

func setSeqNumOnBatchRepr(raw []byte, seq uint64) []byte {
	out := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint64(out[0:8], seq)
	return out
}

func TestOpenWithRecovery_ReplaysEntriesAndRestoresSequence(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := wal.Open(walDir, 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	one := setSeqNumOnBatchRepr(makeBatchRepr(t, "k1", "v1"), 7)
	two := setSeqNumOnBatchRepr(makeBatchRepr(t, "k2", "v2"), 8)
	if err := w.WriteLogEntry(one); err != nil {
		t.Fatalf("write one: %v", err)
	}
	if err := w.WriteLogEntry(two); err != nil {
		t.Fatalf("write two: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	w2, err := wal.Open(walDir, 1)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	t.Cleanup(func() { _ = w2.Close() })

	var seen []uint64
	db, stats, err := OpenWithRecovery(w2, func(e *wal.LogEntry) error {
		seen = append(seen, e.SeqNum)
		return nil
	})
	if err != nil {
		t.Fatalf("open with recovery: %v", err)
	}
	if len(seen) != 2 || seen[0] != 7 || seen[1] != 8 {
		t.Fatalf("replay order/seq mismatch: got %v", seen)
	}
	if stats.RecordsReplayed != 2 {
		t.Fatalf("expected 2 replayed, got %d", stats.RecordsReplayed)
	}

	var next batch.Batch
	if err := next.Put([]byte("k3"), []byte("v3")); err != nil {
		t.Fatalf("batch put next: %v", err)
	}
	if err := db.Write(&next); err != nil {
		t.Fatalf("db write after recovery: %v", err)
	}
	if got := next.SeqNum(); got != 9 {
		t.Fatalf("expected next seq=9 after recovery, got %d", got)
	}
}

func TestOpenWithRecovery_UsesSeqRangeFromCount(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := wal.Open(walDir, 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	var b batch.Batch
	if err := b.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := b.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := b.Put([]byte("k3"), []byte("v3")); err != nil {
		t.Fatalf("put: %v", err)
	}
	b.SetSeqNum(10) // means op seq range is 10..12 when Count=3
	if err := w.WriteLogEntry(b.Repr()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	w2, err := wal.Open(walDir, 1)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	t.Cleanup(func() { _ = w2.Close() })

	db, _, err := OpenWithRecovery(w2, nil)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	var next batch.Batch
	if err := next.Put([]byte("kn"), []byte("vn")); err != nil {
		t.Fatalf("put next: %v", err)
	}
	if err := db.Write(&next); err != nil {
		t.Fatalf("write next: %v", err)
	}
	if got := next.SeqNum(); got != 13 {
		t.Fatalf("expected next start seq=13, got %d", got)
	}
}

func TestOpenWithRecovery_FailsOnMiddleFileCorruption(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := wal.Open(walDir, 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	for i := 1; i <= 3; i++ {
		payload := setSeqNumOnBatchRepr(makeBatchRepr(t, fmt.Sprintf("k%d", i), "v"), uint64(i))
		if err := w.WriteLogEntry(payload); err != nil {
			t.Fatalf("write entry %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	path := filepath.Join(walDir, "000001.log")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	// Flip one byte in the second record payload area (middle corruption).
	// First record occupies: header(7)+payloadLen. So second record payload starts
	// at: firstRecordEnd + header(7).
	firstPayloadLen := int(binary.LittleEndian.Uint16(raw[4:6]))
	firstRecordEnd := 7 + firstPayloadLen
	secondPayloadOff := firstRecordEnd + 7
	raw[secondPayloadOff] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("rewrite wal: %v", err)
	}

	w2, err := wal.Open(walDir, 1)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	t.Cleanup(func() { _ = w2.Close() })

	_, _, err = OpenWithRecovery(w2, nil)
	if err == nil {
		t.Fatalf("expected fatal recovery error for middle corruption")
	}
	if !errors.Is(err, wal.ErrCorruptRecord) {
		t.Fatalf("expected wal.ErrCorruptRecord, got %v", err)
	}
}

func TestRecoverSerializesWithDBMutex(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "wal"), 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	db := Open(w)
	db.mu.Lock()

	done := make(chan struct{})
	go func() {
		_, _ = db.Recover(nil)
		close(done)
	}()

	select {
	case <-done:
		t.Fatalf("recover should block while db mutex is held")
	case <-time.After(50 * time.Millisecond):
	}

	db.mu.Unlock()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatalf("recover did not resume after mutex release")
	}
}

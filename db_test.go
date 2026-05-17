package lsm

import (
	"errors"
	"path/filepath"
	"testing"

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
}

func TestDBWriteNilBatch(t *testing.T) {
	db := Open(nil)
	if err := db.Write(nil); !errors.Is(err, ErrNilBatch) {
		t.Fatalf("expected ErrNilBatch, got %v", err)
	}
}

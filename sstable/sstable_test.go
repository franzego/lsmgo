package sstable

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franzego/lsm-golang/memtable"
)

func TestWritePersistsHeaderAndRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.sst")
	entries := []memtable.Entry{
		{
			Key: memtable.InternalKey{UserKey: []byte("a"), SeqNum: 2, Kind: memtable.KindTombstone},
		},
		{
			Key:   memtable.InternalKey{UserKey: []byte("b"), SeqNum: 1, Kind: memtable.KindPut},
			Value: []byte("value"),
		},
	}

	if err := Write(path, entries); err != nil {
		t.Fatalf("write sstable: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected temp file to be removed after rename, stat err=%v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sstable: %v", err)
	}
	if len(raw) < 8 {
		t.Fatalf("sstable too short: %d", len(raw))
	}
	if got := binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]); got != Version {
		t.Fatalf("version mismatch: got %d", got)
	}
	if string(raw[len(raw)-4:]) != string(Magic[:]) {
		t.Fatalf("footer magic mismatch at end: got %q", raw[len(raw)-4:])
	}

	off := 0
	bodyEnd := len(raw) - 8
	assertRecord(t, raw[:bodyEnd], &off, memtable.KindTombstone, 2, "a", "")
	assertRecord(t, raw[:bodyEnd], &off, memtable.KindPut, 1, "b", "value")
	if off != bodyEnd {
		t.Fatalf("trailing bytes: off=%d len=%d", off, bodyEnd)
	}
}

func assertRecord(t *testing.T, raw []byte, off *int, kind memtable.Kind, seq uint64, key, value string) {
	t.Helper()
	if *off+17 > len(raw) {
		t.Fatalf("record header out of bounds at %d", *off)
	}
	gotKind := memtable.Kind(raw[*off])
	gotSeq := binary.LittleEndian.Uint64(raw[*off+1 : *off+9])
	keyLen := int(binary.LittleEndian.Uint32(raw[*off+9 : *off+13]))
	valueLen := int(binary.LittleEndian.Uint32(raw[*off+13 : *off+17]))
	*off += 17
	if *off+keyLen+valueLen > len(raw) {
		t.Fatalf("record body out of bounds at %d", *off)
	}
	gotKey := string(raw[*off : *off+keyLen])
	*off += keyLen
	gotValue := string(raw[*off : *off+valueLen])
	*off += valueLen

	if gotKind != kind || gotSeq != seq || gotKey != key || gotValue != value {
		t.Fatalf("record = (%d,%d,%q,%q), want (%d,%d,%q,%q)",
			gotKind, gotSeq, gotKey, gotValue, kind, seq, key, value)
	}
}

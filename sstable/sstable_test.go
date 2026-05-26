package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franzego/lsm-golang/memtable"
)

func TestWritePersistsRecordsBloomFilterAndFooter(t *testing.T) {
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

	layout, err := parseFileLayout(raw)
	if err != nil {
		t.Fatalf("parse file layout: %v", err)
	}
	off := 0
	assertRecord(t, layout.records, &off, memtable.KindTombstone, 2, "a", "")
	assertRecord(t, layout.records, &off, memtable.KindPut, 1, "b", "value")
	if off != len(layout.records) {
		t.Fatalf("trailing bytes: off=%d len=%d", off, len(layout.records))
	}
	if !layout.filter.Test([]byte("a")) || !layout.filter.Test([]byte("b")) {
		t.Fatalf("expected bloom filter to contain written keys")
	}
}

func TestGetReturnsValueAndCopiesBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.sst")
	if err := Write(path, []memtable.Entry{
		{
			Key:   memtable.InternalKey{UserKey: []byte("k"), SeqNum: 1, Kind: memtable.KindPut},
			Value: []byte("value"),
		},
	}); err != nil {
		t.Fatalf("write sstable: %v", err)
	}

	got, found, deleted, err := Get(path, []byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found || deleted || string(got) != "value" {
		t.Fatalf("Get(k)=(%q,%v,%v), want value,true,false", got, found, deleted)
	}
	got[0] = 'V'
	again, found, deleted, err := Get(path, []byte("k"))
	if err != nil {
		t.Fatalf("get again: %v", err)
	}
	if !found || deleted || string(again) != "value" {
		t.Fatalf("stored value changed through returned slice: got %q,%v,%v", again, found, deleted)
	}
}

func TestGetReturnsNewestRecordAndTombstone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.sst")
	if err := Write(path, []memtable.Entry{
		{
			Key:   memtable.InternalKey{UserKey: []byte("k"), SeqNum: 1, Kind: memtable.KindPut},
			Value: []byte("old"),
		},
		{
			Key: memtable.InternalKey{UserKey: []byte("k"), SeqNum: 2, Kind: memtable.KindTombstone},
		},
	}); err != nil {
		t.Fatalf("write sstable: %v", err)
	}

	got, found, deleted, err := Get(path, []byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil || !found || !deleted {
		t.Fatalf("Get(k)=(%q,%v,%v), want nil,true,true", got, found, deleted)
	}
}

func TestGetUsesNegativeBloomResultToAvoidRecordScan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.sst")
	if err := Write(path, []memtable.Entry{
		{
			Key:   memtable.InternalKey{UserKey: []byte("present"), SeqNum: 1, Kind: memtable.KindPut},
			Value: []byte("value"),
		},
	}); err != nil {
		t.Fatalf("write sstable: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sstable: %v", err)
	}
	binary.LittleEndian.PutUint32(raw[9:13], ^uint32(0))
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("rewrite sstable: %v", err)
	}

	got, found, deleted, err := Get(path, []byte("missing"))
	if err != nil {
		t.Fatalf("negative bloom lookup should skip corrupt record body: %v", err)
	}
	if got != nil || found || deleted {
		t.Fatalf("Get(missing)=(%q,%v,%v), want nil,false,false", got, found, deleted)
	}
}

func TestGetHandlesBloomFalsePositiveByScanning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.sst")
	if err := Write(path, []memtable.Entry{
		{
			Key:   memtable.InternalKey{UserKey: []byte("present"), SeqNum: 1, Kind: memtable.KindPut},
			Value: []byte("value"),
		},
	}); err != nil {
		t.Fatalf("write sstable: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sstable: %v", err)
	}
	layout, err := parseFileLayout(raw)
	if err != nil {
		t.Fatalf("parse file layout: %v", err)
	}
	filterStart := len(layout.records)
	filterEnd := len(raw) - footerLen
	layout.filter.Add([]byte("missing"))
	var filterBuf bytes.Buffer
	if _, err := layout.filter.WriteTo(&filterBuf); err != nil {
		t.Fatalf("serialize filter: %v", err)
	}
	if filterBuf.Len() != filterEnd-filterStart {
		t.Fatalf("rewritten filter length changed: got %d want %d", filterBuf.Len(), filterEnd-filterStart)
	}
	copy(raw[filterStart:filterEnd], filterBuf.Bytes())
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("rewrite sstable: %v", err)
	}

	got, found, deleted, err := Get(path, []byte("missing"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil || found || deleted {
		t.Fatalf("Get(missing)=(%q,%v,%v), want nil,false,false", got, found, deleted)
	}
}

func TestGetRejectsCorruptFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.sst")
	if err := Write(path, []memtable.Entry{
		{
			Key:   memtable.InternalKey{UserKey: []byte("k"), SeqNum: 1, Kind: memtable.KindPut},
			Value: []byte("v"),
		},
	}); err != nil {
		t.Fatalf("write sstable: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sstable: %v", err)
	}
	copy(raw[len(raw)-4:], []byte("BAD!"))
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("rewrite sstable: %v", err)
	}

	_, _, _, err = Get(path, []byte("k"))
	if !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf("expected ErrCorruptSSTable, got %v", err)
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

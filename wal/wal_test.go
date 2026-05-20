package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type physicalRecord struct {
	rt   RecordType
	data []byte
}

type physicalRecordMeta struct {
	start       int
	payloadOff  int
	payloadSize int
	rt          RecordType
}

func parsePhysicalRecords(raw []byte) ([]physicalRecord, error) {
	var recs []physicalRecord
	offset := 0

	for offset < len(raw) {
		remainInBlock := blockSize - (offset % blockSize)
		if remainInBlock < headerSize {
			for i := 0; i < remainInBlock && offset+i < len(raw); i++ {
				if raw[offset+i] != 0 {
					return nil, errors.New("non-zero padding bytes")
				}
			}
			offset += remainInBlock
			continue
		}

		if offset+headerSize > len(raw) {
			return nil, io.ErrUnexpectedEOF
		}
		h := raw[offset : offset+headerSize]
		checksum := binary.LittleEndian.Uint32(h[0:4])
		payloadLen := int(binary.LittleEndian.Uint16(h[4:6]))
		rt := RecordType(h[6])
		offset += headerSize

		if offset+payloadLen > len(raw) {
			return nil, io.ErrUnexpectedEOF
		}
		payload := raw[offset : offset+payloadLen]
		offset += payloadLen

		if got := computeChecksum(rt, payload); got != checksum {
			return nil, errors.New("checksum mismatch")
		}
		recs = append(recs, physicalRecord{rt: rt, data: append([]byte(nil), payload...)})
	}
	return recs, nil
}

func parsePhysicalRecordMeta(raw []byte) ([]physicalRecordMeta, error) {
	var recs []physicalRecordMeta
	offset := 0
	for offset < len(raw) {
		remainInBlock := blockSize - (offset % blockSize)
		if remainInBlock < headerSize {
			offset += remainInBlock
			continue
		}
		if offset+headerSize > len(raw) {
			return recs, io.ErrUnexpectedEOF
		}
		start := offset
		h := raw[offset : offset+headerSize]
		payloadLen := int(binary.LittleEndian.Uint16(h[4:6]))
		rt := RecordType(h[6])
		offset += headerSize
		if offset+payloadLen > len(raw) {
			return recs, io.ErrUnexpectedEOF
		}
		recs = append(recs, physicalRecordMeta{
			start:       start,
			payloadOff:  offset,
			payloadSize: payloadLen,
			rt:          rt,
		})
		offset += payloadLen
	}
	return recs, nil
}

func makeReplayPayload(seq uint64, count uint32, body []byte) []byte {
	p := make([]byte, 12+len(body))
	binary.LittleEndian.PutUint64(p[0:8], seq)
	binary.LittleEndian.PutUint32(p[8:12], count)
	copy(p[12:], body)
	return p
}

func TestWriteLogEntrySingleRecord(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal"), 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	payload := []byte("hello-wal")
	if err := w.WriteLogEntry(payload); err != nil {
		t.Fatalf("write log entry: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, "wal", "000001.log"))
	if err != nil {
		t.Fatalf("read wal file: %v", err)
	}
	recs, err := parsePhysicalRecords(b)
	if err != nil {
		t.Fatalf("parse wal: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if recs[0].rt != RecordFull {
		t.Fatalf("expected RecordFull, got %d", recs[0].rt)
	}
	if string(recs[0].data) != string(payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestWriteLogEntryFragmentedAcrossBlocks(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal"), 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	payload := make([]byte, blockSize*2+123)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if err := w.WriteLogEntry(payload); err != nil {
		t.Fatalf("write large log entry: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, "wal", "000001.log"))
	if err != nil {
		t.Fatalf("read wal file: %v", err)
	}
	recs, err := parsePhysicalRecords(b)
	if err != nil {
		t.Fatalf("parse wal: %v", err)
	}
	if len(recs) < 2 {
		t.Fatalf("expected fragmented records, got %d", len(recs))
	}
	if recs[0].rt != RecordFirst {
		t.Fatalf("expected first record to be RecordFirst, got %d", recs[0].rt)
	}
	if recs[len(recs)-1].rt != RecordLast {
		t.Fatalf("expected last record to be RecordLast, got %d", recs[len(recs)-1].rt)
	}
	for i := 1; i < len(recs)-1; i++ {
		if recs[i].rt != RecordMiddle {
			t.Fatalf("expected middle fragment at index %d, got %d", i, recs[i].rt)
		}
	}

	combined := make([]byte, 0, len(payload))
	for _, r := range recs {
		combined = append(combined, r.data...)
	}
	if len(combined) != len(payload) {
		t.Fatalf("reassembled length mismatch: got %d want %d", len(combined), len(payload))
	}
	for i := range combined {
		if combined[i] != payload[i] {
			t.Fatalf("payload mismatch at byte %d", i)
		}
	}
}

func TestOpenRestoresBlockOffsetNearBoundary(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := Open(walDir, 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	// write a record that leaves less than header bytes in the current block
	nearBoundary := make([]byte, blockSize-headerSize-3)
	if err := w.WriteLogEntry(nearBoundary); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	w2, err := Open(walDir, 1)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	if err := w2.WriteLogEntry([]byte("next")); err != nil {
		t.Fatalf("second write after reopen: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close wal2: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(walDir, "000001.log"))
	if err != nil {
		t.Fatalf("read wal file: %v", err)
	}
	recs, err := parsePhysicalRecords(b)
	if err != nil {
		t.Fatalf("parse wal: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
}

func TestWriteAfterCloseReturnsErrWALClosed(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal"), 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}
	if err := w.WriteLogEntry([]byte("x")); !errors.Is(err, ErrWALClosed) {
		t.Fatalf("expected ErrWALClosed, got %v", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal"), 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("first close wal: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second close wal: %v", err)
	}
}

func TestParseWALName(t *testing.T) {
	tests := []struct {
		dir    string
		num    uint32
		expect string
	}{
		{dir: "/tmp/wal", num: 1, expect: "/tmp/wal/000001.log"},
		{dir: "/tmp/wal", num: 42, expect: "/tmp/wal/000042.log"},
		{dir: "/tmp/wal", num: 123456, expect: "/tmp/wal/123456.log"},
	}

	for _, tt := range tests {
		got := parseWALName(tt.dir, tt.num)
		if got != tt.expect {
			t.Fatalf("parseWALName(%q,%d)=%q want %q", tt.dir, tt.num, got, tt.expect)
		}
	}
}

func TestConcurrentWriteLogEntryNoCorruption(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal"), 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	const writers = 64
	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 0; i < writers; i++ {
		i := i
		go func() {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("writer-%03d", i))
			if err := w.WriteLogEntry(payload); err != nil {
				t.Errorf("write %d failed: %v", i, err)
			}
		}()
	}
	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, "wal", "000001.log"))
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	recs, err := parsePhysicalRecords(b)
	if err != nil {
		t.Fatalf("parse wal: %v", err)
	}
	if len(recs) != writers {
		t.Fatalf("record count mismatch: got %d want %d", len(recs), writers)
	}

	seen := make(map[string]bool, writers)
	for _, r := range recs {
		if r.rt != RecordFull {
			t.Fatalf("expected RecordFull for tiny concurrent payload, got %d", r.rt)
		}
		seen[string(r.data)] = true
	}
	for i := 0; i < writers; i++ {
		want := fmt.Sprintf("writer-%03d", i)
		if !seen[want] {
			t.Fatalf("missing payload %q", want)
		}
	}
}

func BenchmarkWALWriteLogEntry(b *testing.B) {
	sizes := []int{128, 1024, 8 * 1024, 32 * 1024, 128 * 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("payload_%dB", size), func(b *testing.B) {
			dir := b.TempDir()
			w, err := Open(filepath.Join(dir, "wal"), 1)
			if err != nil {
				b.Fatalf("open wal: %v", err)
			}
			defer func() { _ = w.Close() }()

			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 251)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.WriteLogEntry(payload); err != nil {
					b.Fatalf("write log entry: %v", err)
				}
			}
		})
	}
}

func TestReplayStopsOnTruncatedTailNonFatal(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := Open(walDir, 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.WriteLogEntry(makeReplayPayload(1, 1, []byte("ok"))); err != nil {
		t.Fatalf("write entry: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Append incomplete header bytes to simulate crash-tail truncation.
	f, err := os.OpenFile(filepath.Join(walDir, "000001.log"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open wal for append: %v", err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatalf("append partial tail: %v", err)
	}
	_ = f.Close()

	w2, err := Open(walDir, 1)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer func() { _ = w2.Close() }()

	var got int
	stats, err := w2.Replay(func(e *LogEntry) error {
		got++
		return nil
	})
	if err != nil {
		t.Fatalf("replay err: %v", err)
	}
	if got != 1 || stats.RecordsReplayed != 1 {
		t.Fatalf("expected 1 replayed record, got=%d stats=%d", got, stats.RecordsReplayed)
	}
	if stats.StopReason != "truncated_tail" {
		t.Fatalf("expected stop reason truncated_tail, got %q", stats.StopReason)
	}
}

func TestReplayChecksumMismatchTailNonFatal(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := Open(walDir, 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.WriteLogEntry(makeReplayPayload(1, 1, []byte("first"))); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if err := w.WriteLogEntry(makeReplayPayload(2, 1, []byte("second"))); err != nil {
		t.Fatalf("write second: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	path := filepath.Join(walDir, "000001.log")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	meta, err := parsePhysicalRecordMeta(raw)
	if err != nil {
		t.Fatalf("parse meta: %v", err)
	}
	last := meta[len(meta)-1]
	raw[last.payloadOff] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}

	w2, err := Open(walDir, 1)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer func() { _ = w2.Close() }()

	var got int
	stats, err := w2.Replay(func(e *LogEntry) error {
		got++
		return nil
	})
	if err != nil {
		t.Fatalf("replay err: %v", err)
	}
	if got != 1 {
		t.Fatalf("expected only first entry replayed, got %d", got)
	}
	if stats.StopReason != "checksum_mismatch_tail" {
		t.Fatalf("expected checksum_mismatch_tail, got %q", stats.StopReason)
	}
}

func TestReplayChecksumMismatchMiddleFatal(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	w, err := Open(walDir, 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if err := w.WriteLogEntry(makeReplayPayload(uint64(i), 1, []byte(fmt.Sprintf("entry-%d", i)))); err != nil {
			t.Fatalf("write entry %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	path := filepath.Join(walDir, "000001.log")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	meta, err := parsePhysicalRecordMeta(raw)
	if err != nil {
		t.Fatalf("parse meta: %v", err)
	}
	if len(meta) < 3 {
		t.Fatalf("expected at least 3 physical records")
	}
	mid := meta[1]
	raw[mid.payloadOff] ^= 0xAA
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}

	w2, err := Open(walDir, 1)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer func() { _ = w2.Close() }()

	var got int
	_, err = w2.Replay(func(e *LogEntry) error {
		got++
		return nil
	})
	if err == nil {
		t.Fatalf("expected fatal corruption error")
	}
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("expected ErrCorruptRecord, got %v", err)
	}
	if got != 1 {
		t.Fatalf("expected replay to stop at middle corruption after 1 entry, got %d", got)
	}
}

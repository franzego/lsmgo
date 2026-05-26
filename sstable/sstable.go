package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/franzego/lsm-golang/memtable"
)

var (
	// Magic is written at the end of the file so readers can seek from EOF
	// and validate the SSTable before parsing the body.
	Magic   = [4]byte{'L', 'S', 'M', 'S'}
	Version = uint32(1)

	ErrEmptyPath      = errors.New("sstable: empty path")
	ErrCorruptSSTable = errors.New("sstable: corrupt sstable")
)

const (
	recordHeaderLen = 17 // kind(1) + seq(8) + keyLen(4) + valueLen(4)
	footerLen       = 24 // recordEnd(8) + filterLen(8) + version(4) + magic(4)

	defaultFalsePositiveRate = 0.01
)

// Write persists entries to path using the current minimal SSTable format.
// The caller supplies entries in internal-key order.
//
// File layout:
//
//	[record bytes][serialized bloom filter][fixed footer]
//
// The footer is fixed-size and ends with Magic, so readers can validate the
// file by looking at the tail before interpreting the variable-size sections.
func Write(path string, entries []memtable.Entry) error {
	dir := filepath.Dir(path)
	if path == "" {
		return ErrEmptyPath
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmpPath := path + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()

	var recordBuf [17]byte
	for _, entry := range entries {
		recordBuf[0] = byte(entry.Key.Kind)
		binary.LittleEndian.PutUint64(recordBuf[1:9], entry.Key.SeqNum)
		binary.LittleEndian.PutUint32(recordBuf[9:13], uint32(len(entry.Key.UserKey)))
		binary.LittleEndian.PutUint32(recordBuf[13:17], uint32(len(entry.Value)))
		if _, err := f.Write(recordBuf[:]); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.Write(entry.Key.UserKey); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.Write(entry.Value); err != nil {
			_ = f.Close()
			return err
		}
	}

	recordEnd, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		_ = f.Close()
		return err
	}

	filter := bloom.NewWithEstimates(bloomCapacity(len(entries)), defaultFalsePositiveRate)
	for _, entry := range entries {
		filter.Add(entry.Key.UserKey)
	}
	var filterBuf bytes.Buffer
	if _, err := filter.WriteTo(&filterBuf); err != nil {
		_ = f.Close()
		return err
	}
	filterBytes := filterBuf.Bytes()
	if _, err := f.Write(filterBytes); err != nil {
		_ = f.Close()
		return err
	}

	var footerBuf [footerLen]byte
	binary.LittleEndian.PutUint64(footerBuf[0:8], uint64(recordEnd))
	binary.LittleEndian.PutUint64(footerBuf[8:16], uint64(len(filterBytes)))
	binary.LittleEndian.PutUint32(footerBuf[16:20], Version)
	copy(footerBuf[20:24], Magic[:])
	if _, err := f.Write(footerBuf[:]); err != nil {
		_ = f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	committed = true
	return nil
}

// Get looks up key in one SSTable file.
//
// It returns (value, found, deleted, error):
//   - found=false means the key is not present in this SSTable.
//   - found=true and deleted=false means value contains the newest value.
//   - found=true and deleted=true means the newest record is a tombstone.
//
// Lookup flow:
//  1. Read the whole file. (Not very efficient for now as blocks have not been introduced yet.)
//  2. Validate the footer and load the Bloom filter.
//  3. If the Bloom filter says "definitely not present", stop immediately.
//  4. Otherwise scan records, keep the matching record with the highest seqnum,
//     and interpret tombstones as deletes.
func Get(path string, key []byte) ([]byte, bool, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, false, false, err
	}
	layout, err := parseFileLayout(raw)
	if err != nil {
		return nil, false, false, err
	}
	if !layout.filter.Test(key) {
		return nil, false, false, nil
	}

	var newest memtable.Entry
	found := false
	for off := 0; off < len(layout.records); {
		entry, next, err := parseRecordAt(layout.records, off)
		if err != nil {
			return nil, false, false, err
		}
		off = next
		if bytes.Compare(entry.Key.UserKey, key) != 0 {
			continue
		}
		if !found || entry.Key.SeqNum > newest.Key.SeqNum {
			newest = entry
			found = true
		}
	}
	if !found {
		return nil, false, false, nil
	}
	if newest.Key.Kind == memtable.KindTombstone {
		return nil, true, true, nil
	}
	return append([]byte(nil), newest.Value...), true, false, nil
}

type fileLayout struct {
	records []byte
	filter  *bloom.BloomFilter
}

// parseFileLayout validates the fixed footer and splits the SSTable into its
// records section and serialized Bloom filter section. It does not parse every
// record; records are decoded lazily only if the filter says the key may exist.
func parseFileLayout(raw []byte) (fileLayout, error) {
	if len(raw) < footerLen {
		return fileLayout{}, ErrCorruptSSTable
	}
	footer := raw[len(raw)-footerLen:]
	if !bytes.Equal(footer[20:24], Magic[:]) {
		return fileLayout{}, ErrCorruptSSTable
	}
	if binary.LittleEndian.Uint32(footer[16:20]) != Version {
		return fileLayout{}, ErrCorruptSSTable
	}

	recordEnd := int(binary.LittleEndian.Uint64(footer[0:8]))
	filterLen := int(binary.LittleEndian.Uint64(footer[8:16]))
	filterStart := recordEnd
	filterEnd := len(raw) - footerLen
	if recordEnd < 0 || filterStart > filterEnd || filterEnd-filterStart != filterLen {
		return fileLayout{}, ErrCorruptSSTable
	}
	filter := &bloom.BloomFilter{}
	r := bytes.NewReader(raw[filterStart:filterEnd])
	if _, err := filter.ReadFrom(r); err != nil {
		return fileLayout{}, ErrCorruptSSTable
	}
	if r.Len() != 0 {
		return fileLayout{}, ErrCorruptSSTable
	}
	return fileLayout{
		records: raw[:recordEnd],
		filter:  filter,
	}, nil
}

// parseRecordAt decodes one record starting at off and returns the offset of
// the next record. Records have a fixed header followed by variable-size key
// and value bytes.
func parseRecordAt(raw []byte, off int) (memtable.Entry, int, error) {
	if off+recordHeaderLen > len(raw) {
		return memtable.Entry{}, 0, ErrCorruptSSTable
	}
	kind := memtable.Kind(raw[off])
	seq := binary.LittleEndian.Uint64(raw[off+1 : off+9])
	keyLen := int(binary.LittleEndian.Uint32(raw[off+9 : off+13]))
	valueLen := int(binary.LittleEndian.Uint32(raw[off+13 : off+17]))
	off += recordHeaderLen
	if keyLen < 0 || valueLen < 0 || off+keyLen+valueLen > len(raw) {
		return memtable.Entry{}, 0, ErrCorruptSSTable
	}
	key := append([]byte(nil), raw[off:off+keyLen]...)
	off += keyLen
	value := append([]byte(nil), raw[off:off+valueLen]...)
	off += valueLen
	return memtable.Entry{
		Key: memtable.InternalKey{
			UserKey: key,
			SeqNum:  seq,
			Kind:    kind,
		},
		Value: value,
	}, off, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func bloomCapacity(entryCount int) uint {
	if entryCount < 1 {
		return 1
	}
	return uint(entryCount)
}

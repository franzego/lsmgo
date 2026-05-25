package sstable

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"

	"github.com/franzego/lsm-golang/memtable"
)

var (
	// Magic is written at the end of the file so readers can seek from EOF
	// and validate the SSTable before parsing the body.
	Magic   = [4]byte{'L', 'S', 'M', 'S'}
	Version = uint32(1)

	ErrEmptyPath = errors.New("sstable: empty path")
)

// Write persists entries to path using the current minimal SSTable format.
// The caller supplies entries in internal-key order.
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

	var footerBuf [4]byte
	binary.LittleEndian.PutUint32(footerBuf[:], Version)
	if _, err := f.Write(footerBuf[:]); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(Magic[:]); err != nil {
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

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/franzego/lsm-golang/fs"
)

type WAL struct {
	mu          sync.Mutex
	fs          fs.FS
	file        fileLike
	walNum      uint32
	dir         fileLike
	dirname     string
	blockOffset int // current position within the 32KB block
	closed      bool
}

var ErrWALClosed = errors.New("wal: closed")

type fileLike interface {
	Sync() error
	Close() error
	Write(p []byte) (n int, err error)
	ReadAt(p []byte, off int64) (n int, err error)
	Stat() (os.FileInfo, error)
}

func parseWALName(dir string, walNum uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.log", walNum))
}

func Open(dirpath string, walNum uint32) (*WAL, error) {
	return OpenWithFS(dirpath, walNum, fs.DefaultFS())
}

func OpenWithFS(dirpath string, walNum uint32, filesystem fs.FS) (*WAL, error) {
	if err := filesystem.MkdirAll(dirpath, 0o755); err != nil {
		return nil, err
	}
	directory, err := filesystem.Open(dirpath)
	if err != nil {
		return nil, err
	}
	wal := parseWALName(dirpath, walNum)
	file, err := filesystem.OpenFile(wal, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		directory.Close()
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		directory.Close()
		return nil, err
	}
	blockOffset := int(info.Size() % blockSize)

	return &WAL{
		fs:          filesystem,
		file:        file,
		walNum:      walNum,
		dir:         directory,
		dirname:     dirpath,
		blockOffset: blockOffset,
	}, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	if err := w.dir.Close(); err != nil {
		return err
	}
	w.closed = true
	return nil
}

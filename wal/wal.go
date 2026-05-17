package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/franzego/lsm-golang/fs"
)

type WAL struct {
	fs          fs.FS
	file        *os.File
	walNum      uint32
	dir         *os.File
	dirname     string
	blockOffset int // current position within the 32KB block
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
	directory, err := os.Open(dirpath)
	if err != nil {
		return nil, err
	}
	wal := parseWALName(dirpath, walNum)
	file, err := os.OpenFile(wal, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
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
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	return w.dir.Close()
}

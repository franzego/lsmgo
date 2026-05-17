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

func dirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		return false
	}
	return info.IsDir() // true only if it's a directory, not a file
}

func Open(dirpath string, walNum uint32) (*WAL, error) {
	if !dirExists(dirpath) {
		if err := os.Mkdir(dirpath, 0755); err != nil {
			return nil, err
		}
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
	return &WAL{
		fs:          nil,
		file:        file,
		walNum:      walNum,
		dir:         directory,
		dirname:     dirpath,
		blockOffset: 0,
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

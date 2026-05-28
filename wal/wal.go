package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/franzego/lsm-golang/fs"
	ilog "github.com/franzego/lsm-golang/internal/log"
)

type WAL struct {
	fs      fs.FS
	file    fileLike
	walNum  uint32
	dir     fileLike
	dirname string
	writer  *ilog.Writer
}

var ErrWALClosed = errors.New("wal: closed")
var ErrCorruptRecord = errors.New("wal: corrupt record")

type fileLike interface {
	ilog.FileLike
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
	writer, err := ilog.NewWriter(file, directory)
	if err != nil {
		file.Close()
		directory.Close()
		return nil, err
	}

	return &WAL{
		fs:      filesystem,
		file:    file,
		walNum:  walNum,
		dir:     directory,
		dirname: dirpath,
		writer:  writer,
	}, nil
}

func (w *WAL) Close() error {
	return w.writer.Close()
}

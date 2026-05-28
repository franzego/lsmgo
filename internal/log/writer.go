package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
)

var ErrClosed = errors.New("log: closed")

type FileLike interface {
	Sync() error
	Close() error
	Write(p []byte) (n int, err error)
	ReadAt(p []byte, off int64) (n int, err error)
	Stat() (os.FileInfo, error)
}

type Writer struct {
	mu          sync.Mutex
	file        FileLike
	dir         FileLike
	blockOffset int
	closed      bool
}

func NewWriter(file, dir FileLike) (*Writer, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &Writer{
		file:        file,
		dir:         dir,
		blockOffset: int(info.Size() % BlockSize),
	}, nil
}

func (w *Writer) Write(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	remaining := BlockSize - w.blockOffset
	if remaining < RecordHeaderSize {
		padding := make([]byte, remaining)
		if _, err := w.writeAll(padding); err != nil {
			return err
		}
		w.blockOffset = 0
		remaining = BlockSize
	}
	if RecordHeaderSize+len(data) <= remaining {
		if err := w.writeRecord(data, RecordFull); err != nil {
			return err
		}
		return w.sync()
	}
	return w.writeFragmented(data)
}

func (w *Writer) Close() error {
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

func (w *Writer) writeRecord(data []byte, rt RecordType) error {
	checksum := ComputeChecksum(rt, data)
	header := make([]byte, RecordHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint16(header[4:6], uint16(len(data)))
	header[6] = byte(rt)
	record := make([]byte, RecordHeaderSize+len(data))
	copy(record[0:RecordHeaderSize], header)
	copy(record[RecordHeaderSize:], data)
	n, err := w.writeAll(record)
	if err != nil {
		return err
	}
	w.blockOffset += n
	return nil
}

func (w *Writer) writeAll(data []byte) (int, error) {
	written := 0
	for written < len(data) {
		n, err := w.file.Write(data[written:])
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, fmt.Errorf("log: short write: wrote 0 of %d bytes remaining", len(data)-written)
		}
		written += n
	}
	return written, nil
}

func (w *Writer) writeFragmented(data []byte) error {
	first := true
	for len(data) > 0 {
		remaining := BlockSize - w.blockOffset
		if remaining < RecordHeaderSize {
			padding := make([]byte, remaining)
			if _, err := w.writeAll(padding); err != nil {
				return err
			}
			w.blockOffset = 0
			remaining = BlockSize
		}

		chunkSize := remaining - RecordHeaderSize
		isLast := chunkSize >= len(data)
		if isLast {
			chunkSize = len(data)
		}

		var rt RecordType
		switch {
		case first && isLast:
			rt = RecordFull
		case first:
			rt = RecordFirst
		case isLast:
			rt = RecordLast
		default:
			rt = RecordMiddle
		}

		if err := w.writeRecord(data[:chunkSize], rt); err != nil {
			return err
		}
		data = data[chunkSize:]
		first = false
	}
	return w.sync()
}

func (w *Writer) sync() error {
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.dir.Sync()
}

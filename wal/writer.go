package wal

import (
	"errors"

	ilog "github.com/franzego/lsm-golang/internal/log"
)

func (w *WAL) WriteLogEntry(data []byte) error {
	if err := w.writer.Write(data); err != nil {
		if errors.Is(err, ilog.ErrClosed) {
			return ErrWALClosed
		}
		return err
	}
	return nil
}

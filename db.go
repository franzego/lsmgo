package lsm

import (
	"sync/atomic"

	"github.com/franzego/lsm-golang/batch"
)

type DB struct {
	seqNum  atomic.Uint64
	BatchDb batch.Batch
}

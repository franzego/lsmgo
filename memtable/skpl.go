package memtable

import (
	"bytes"

	"github.com/huandu/skiplist"
)

func newSkipList() *skiplist.SkipList {
	return skiplist.New(skiplist.GreaterThanFunc(func(lhs, rhs interface{}) int {
		left := lhs.(InternalKey)
		right := rhs.(InternalKey)

		if c := compareUserKeys(left.UserKey, right.UserKey); c != 0 {
			return c
		}

		// Descending sequence for same user key.
		if left.SeqNum > right.SeqNum {
			return -1
		}
		if left.SeqNum < right.SeqNum {
			return 1
		}

		if left.Kind < right.Kind {
			return -1
		}
		if left.Kind > right.Kind {
			return 1
		}
		return 0
	}))
}

func compareUserKeys(lhs, rhs []byte) int {
	return bytes.Compare(lhs, rhs)
}

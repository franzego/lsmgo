package fs

import "fmt"

// A DiskFileNum identifies a file or object with exists on disk.
type DiskFileNum uint64

// String implements fmt.Stringer.
func (dfn DiskFileNum) String() string {
	return fmt.Sprintf("%06d", uint64(dfn))
}

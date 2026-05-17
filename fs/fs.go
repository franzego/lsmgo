package fs

import "os"

type FS interface {
	MkdirAll(dir string, perm os.FileMode) error
	Rename(oldName, newName string) error
	Remove(name string) error
	RemoveAll(name string) error
}

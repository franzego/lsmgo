package fs

import "os"

type FS interface {
	MkdirAll(dir string, perm os.FileMode) error
	Open(name string) (*os.File, error)
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	Stat(name string) (os.FileInfo, error)
	Rename(oldName, newName string) error
	Remove(name string) error
	RemoveAll(name string) error
}

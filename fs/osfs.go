package fs

import "os"

type diskFS struct{}

func DefaultFS() FS {
	return diskFS{}
}

func (diskFS) MkdirAll(dir string, perm os.FileMode) error {
	return os.MkdirAll(dir, perm)
}

func (diskFS) Open(name string) (*os.File, error) {
	return os.Open(name)
}

func (diskFS) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (diskFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (diskFS) Rename(oldName, newName string) error {
	return os.Rename(oldName, newName)
}

func (diskFS) Remove(name string) error {
	return os.Remove(name)
}

func (diskFS) RemoveAll(name string) error {
	return os.RemoveAll(name)
}

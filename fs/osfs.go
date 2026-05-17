package fs

import "os"

type diskFS struct{}

func DefaultFS() FS {
	return diskFS{}
}

func (diskFS) MkdirAll(dir string, perm os.FileMode) error {
	return os.MkdirAll(dir, perm)
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

func (diskFS) Close() error {
	return nil
}

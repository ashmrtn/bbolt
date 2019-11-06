package bbolt

import (
	"os"
	"time"
)

type File interface {
	Sync() error
	Fd() uintptr
	Truncate(size int64) error
	Name() string
	WriteAt(b []byte, off int64) (int, error)
	Stat() (os.FileInfo, error)
	ReadAt(b []byte, off int64) (int, error)
	Close() error
	Read(b []byte) (int, error)
	Write(b []byte) (int, error)
	Seek(offset int64, whence int) (int64, error)
}

type SysFuncs interface {
	flock(db *DB, exclusive bool, timeout time.Duration) error
	funlock(db *DB) error
	mmap(db *DB, sz int) error
	munmap(db *DB) error
	madvise(b []byte, advice int) (err error)
	fdatasync(db *DB) error
}

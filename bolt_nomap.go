package bbolt

import (
	"errors"
	"time"
	"unsafe"

	"go.ashmrtn/bbolt/spdk"
)

var NoMap noMap

type noMap struct{}

func (*noMap) flock(db *DB, exclusive bool, timeout time.Duration) error {
	return nil
}

func (*noMap) funlock(db *DB) error {
	return nil
}

func (*noMap) mmap(db *DB, sz int) error {
	spdkF, ok := db.file.(*spdk.SpdkFile)
	if !ok {
		return errors.New("Nomap only usable with SpdkFile")
	}
	b, err := spdkF.Mmap(sz)
	if err != nil {
		return err
	}

	db.dataref = b
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	db.datasz = sz
	return nil
}

func (*noMap) munmap(db *DB) error {
	spdkF, ok := db.file.(*spdk.SpdkFile)
	if !ok {
		return errors.New("Nomap only usable with SpdkFile")
	}
	spdkF.Munmap()
	db.dataref = nil
	db.data = nil
	db.datasz = 0
	return nil
}

func (*noMap) madvise(b []byte, advice int) (err error) {
	return nil
}

func (*noMap) fdatasync(db *DB) error {
	return db.file.Sync()
}

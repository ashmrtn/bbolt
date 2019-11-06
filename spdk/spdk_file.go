package spdk

// #cgo CFLAGS: -g -O2 -Wall -Wextra -Wno-unused-parameter -Wno-missing-field-initializers -fno-strict-aliasing -fPIC -fstack-protector -fno-common -Wformat -Wformat-security -I/spdk/include
// #cgo LDFLAGS: -pthread -Wl,-z,relro,-z,now -Wl,-z,noexecstack -L/spdk/build/lib -Wl,--whole-archive -lspdk_log -lspdk_nvme -lspdk_sock -lspdk_sock_posix -lspdk_thread -lspdk_vmd -lspdk_util -Wl,--no-whole-archive /spdk/build/lib/libspdk_env_dpdk.a -Wl,--whole-archive /spdk/dpdk/build/lib/librte_eal.a /spdk/dpdk/build/lib/librte_mempool.a /spdk/dpdk/build/lib/librte_ring.a /spdk/dpdk/build/lib/librte_mbuf.a /spdk/dpdk/build/lib/librte_mempool_ring.a /spdk/dpdk/build/lib/librte_pci.a /spdk/dpdk/build/lib/librte_bus_pci.a /spdk/dpdk/build/lib/librte_kvargs.a /spdk/dpdk/build/lib/librte_vhost.a /spdk/dpdk/build/lib/librte_net.a /spdk/dpdk/build/lib/librte_hash.a /spdk/dpdk/build/lib/librte_cryptodev.a -Wl,--no-whole-archive -lnuma -ldl -luuid
// #include <stdlib.h>
// #include "spdk_file.h"
import "C"
import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unsafe"
)

// Use a 4k block because Optane has better parallelism with that size.
const metaSize = 4096

type SpdkFile struct {
	proto   string
	devAddr string
	// This should be treated as an opaque field, but is required to live for the
	// lifetime of the database.
	ctx    C.struct_SpdkCtx
	queued *list.List

	// Go only information.
	offset int64
	size   int64

	mmapBuf []byte
}

func (f *SpdkFile) Mmap(sz int) ([]byte, error) {
	b := make([]byte, sz)
	// Just read everything in from spdk. This is the equivalent to telling the
	// kernel to pre-fault the entire mmap range.
	read, err := f.ReadAt(b, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	// We reached the end of the file, but still want our mmap to be large
	// enough. Just zero fill the rest of the buffer in this case.
	if read != sz {
		fillBuf(0, b[read:])
	}

	f.mmapBuf = b
	return f.mmapBuf, nil
}

func (f *SpdkFile) Munmap() {
	// Free reference for GC later.
	f.mmapBuf = nil
}

func OpenFile(path string, flags int, mode os.FileMode) (*SpdkFile, error) {
	f := &SpdkFile{}
	parts := strings.Split(path, " ")
	if len(parts) != 3 {
		return nil, errors.New("Requires 3 file parts")
	}
	f.proto = parts[1]
	f.devAddr = parts[2]

	// Some sad calls into C land to initalize spdk.
	trid := C.CString("trtype=" + f.proto + " traddr=" + f.devAddr)
	defer C.free(unsafe.Pointer(trid))
	res := C.SpdkInit(trid, &f.ctx)
	if res != 0 {
		return nil, errors.New("Unable to initialize spdk")
	}
	f.queued = list.New()

	// Try to read file size from device.
	blk := make([]byte, metaSize)
	f.readAt(blk, 0)
	f.size = int64(binary.BigEndian.Uint64(blk[0:]))

	// Small amount of sanity testing here for kicks.
	oldSize := f.size
	f.WriteAt([]byte(strings.Repeat("o", 4096)), oldSize)
	//f.Sync()
	d := make([]byte, 8192)
	_, err := f.ReadAt(d, oldSize)
	if err != nil {
		if err == io.EOF {
			fmt.Printf("%s\n", err.Error())
		} else {
			return nil, err
		}
	}
	fmt.Printf("%s\n\n", string(d))
	f.Close()

	// TODO(ashmrtnz): Uncomment when actually working.
	//return f, nil
	return nil, errors.New("Not implemented")
}

// TODO(ashmrtnz): Make thread safe.
// Assumes that backing device has PLP and therefore does not require a flush
// operation.
func (f *SpdkFile) Sync() error {
	newSize := f.size
	for pending := f.queued.Len(); pending > 0; pending = f.queued.Len() {
		done := int(C.ProcessCompletions(&f.ctx, C.uint(pending)))
		// Remove requests that were completed and update the file size if needed.
		for i := 0; i < done; i++ {
			first := f.queued.Front()
			iou := first.Value.(*C.struct_Iou)
			tmp := int64(iou.offset+C.ulonglong(iou.bufSize)) - metaSize
			if iou.ioType == C.SpdkWrite && tmp > newSize {
				newSize = tmp
			}
			C.spdk_free(unsafe.Pointer(iou.buf))
			f.queued.Remove(first)
		}
	}
	// Queue a write with the updated file size and wait for it to complete.
	var err error
	if newSize > f.size {
		err = f.updateFileSize(newSize)
	}

	return err
}

func (f *SpdkFile) Fd() uintptr {
	return 0
}

func (f *SpdkFile) Truncate(size int64) error {
	if size < 0 {
		return &os.PathError{
			Op:   "truncate",
			Path: f.Name(),
			Err:  os.ErrInvalid,
		}
	}

	// Queue a write zero request, this will be sync-ed to disk in
	// updateFileSize().
	if size > f.size {
		iou, err := f.initIou(C.SpdkWriteZeroes, f.size, int(size-f.size))
		if err != nil {
			return &os.PathError{
				Op:   "truncate",
				Path: f.Name(),
				Err:  err,
			}
		}

		if res := C.QueueIO(&f.ctx, iou, (*C.char)(nil)); res != 0 {
			return &os.PathError{
				Op:   "truncate",
				Path: f.Name(),
				Err:  errors.New("Unable to queue IO with spdk"),
			}
		}
		f.queued.PushBack(iou)

		// Update the "mmap" file.
		if f.mmapBuf != nil {
			fillBuf(0, f.mmapBuf[f.size:])
		}
	}
	return f.updateFileSize(size)
}

func (f *SpdkFile) Name() string {
	return "spdk-file"
}

func (f *SpdkFile) Stat() (os.FileInfo, error) {
	return os.FileInfo(f), nil
}

func (f *SpdkFile) Size() int64 {
	return f.size
}

func (f *SpdkFile) Mode() os.FileMode {
	return os.ModePerm
}

func (f *SpdkFile) ModTime() time.Time {
	return time.Now()
}

func (f *SpdkFile) IsDir() bool {
	return false
}

func (f *SpdkFile) Sys() interface{} {
	return nil
}

func (f *SpdkFile) writeAt(b []byte, off int64) (int, error) {
	iou, err := f.initIou(C.SpdkWrite, off, len(b))
	if err != nil {
		return -1, err
	}

	// TODO(ashmrtnz): Remove the following call eventually.
	// Sad call that causes an extra data copy.
	ptr := C.CBytes(b)
	defer C.free(ptr)
	if res := C.QueueIO(&f.ctx, iou, (*C.char)(ptr)); res != 0 {
		return -1, errors.New("Unable to queue IO with spdk")
	}

	f.queued.PushBack(iou)

	return int(iou.bufSize), nil
}

func (f *SpdkFile) WriteAt(b []byte, off int64) (int, error) {
	res, err := f.writeAt(b, off+metaSize)
	if err != nil {
		return 0, err
	}

	// Copy data to our mmap so that subsequent reads can see it. Mmap is
	// always from the start of the file, so we can just use the same
	// offset.
	if f.mmapBuf != nil {
		copy(f.mmapBuf[off:], b)
	}
	return res, err
}

func (f *SpdkFile) readAt(b []byte, off int64) (int, error) {
	iou, err2 := f.initIou(C.SpdkRead, off, len(b))
	if err2 != nil {
		return -1, err2
	}

	// TODO(ashmrtnz): Remove the following call eventually.
	// Sad call that causes an extra data copy.
	ptr := C.CBytes(b)
	defer C.free(ptr)
	if res := C.QueueIO(&f.ctx, iou, (*C.char)(ptr)); res != 0 {
		return -1, errors.New("Unable to queue IO with spdk")
	}
	f.queued.PushBack(iou)

	// Assume that only a single read request is outstanding at a time, otherwise
	// data will be lost because buffers won't be retrieved properly.
	err := f.waitForRead(iou, b)
	if err != nil {
		return -1, err
	}

	return len(b), err
}

func (f *SpdkFile) ReadAt(b []byte, off int64) (int, error) {
	// This is kind of a cop out to get the file size updated before we queue the
	// request.
	f.Sync()

	size := len(b)
	var err error

	if off >= f.size {
		return 0, io.EOF
	} else if off+int64(len(b)) > f.size {
		size = int(f.size - off)
		err = io.EOF
	}

	// Add metaSize here to skip over our "file metadata".
	read, err2 := f.readAt(b[:size], off+metaSize)
	if err2 != nil {
		err = err2
	}
	return read, err
}

// TODO(ashmrtnz): Should probably move spdk teardown to a specific function
// because a file may be closed without exiting the application. Similarly for
// registering spdk stuff at file open.
func (f *SpdkFile) Close() error {
	C.SpdkTeardown(&f.ctx)
	return nil
}

func (f *SpdkFile) Read(b []byte) (int, error) {
	return f.ReadAt(b, f.offset)
}

func (f *SpdkFile) Write(b []byte) (int, error) {
	return f.WriteAt(b, f.offset)
}

func (f *SpdkFile) Seek(offset int64, whence int) (int64, error) {
	newOff := int64(0)
	switch whence {
	case 0:
		newOff = offset
	case 1:
		newOff = f.offset + offset
	case 2:
		newOff = f.size + offset
	}
	if newOff < 0 {
		return f.offset, os.ErrInvalid
	}
	f.offset = newOff
	return f.offset, nil
}

func (f *SpdkFile) initIou(opType C.enum_IoType, off int64,
	size int) (*C.struct_Iou, error) {
	iou := C.struct_Iou{
		ioType:   opType,
		bufSize:  C.ulong(size),
		offset:   C.ulonglong(off),
		lba:      C.ulonglong(off) / C.ulonglong(f.ctx.SectorSize),
		lbaCount: C.ulong(size) / C.ulong(f.ctx.SectorSize),
	}

	if int64(iou.lba)*int64(f.ctx.SectorSize) != off {
		return nil, errors.New("Offset not sector aligned")
	}
	if int(iou.lbaCount)*int(f.ctx.SectorSize) != size {
		return nil, errors.New("Length not a multiple of sector size")
	}
	return &iou, nil
}

func (f *SpdkFile) waitForRead(iou *C.struct_Iou, out []byte) error {
	idx := 0
	for a := f.queued.Front(); a != nil; a = a.Next() {
		if a.Value == iou {
			break
		}
		idx++
	}
	if idx == f.queued.Len() {
		return errors.New("Iou not queued")
	}

	// Complete all but our read.
	for completed := 0; completed < idx; {
		done := int(C.ProcessCompletions(&f.ctx, C.uint((idx+1)-completed)))
		for i := 0; i < done; i++ {
			first := f.queued.Front()
			iou := first.Value.(*C.struct_Iou)
			C.spdk_free(unsafe.Pointer(iou.buf))
			f.queued.Remove(first)
		}
		completed += done
	}

	// Wait for our read to complete.
	for {
		ioDone := C.ProcessCompletions(&f.ctx, 1)
		if ioDone != 0 {
			break
		}
	}

	// Copy our data out of the buffer.
	first := f.queued.Front()
	finalIou := first.Value.(*C.struct_Iou)
	copy(out, C.GoBytes(unsafe.Pointer(finalIou.buf), C.int(finalIou.bufSize)))

	C.spdk_free(unsafe.Pointer(finalIou.buf))
	f.queued.Remove(first)

	return nil
}

func makeBlock(data int64, blk []byte) {
	headerSize := 8
	binary.BigEndian.PutUint64(blk[0:], uint64(data))

	// Fill the rest of the block with zeros.
	fillBuf(0, blk[headerSize:])
}

func (f *SpdkFile) updateFileSize(newSize int64) error {
	blk := make([]byte, metaSize)
	makeBlock(newSize, blk)
	_, err := f.writeAt(blk, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	f.size = newSize
	return nil
}

func fillBuf(c byte, blk []byte) {
	blk[0] = c
	for i := 1; i < len(blk); i *= 2 {
		copy(blk[i:], blk[:i])
	}
}

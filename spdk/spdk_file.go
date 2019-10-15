package spdk

// #cgo CFLAGS: -g -O2 -Wall -Wextra -Wno-unused-parameter -Wno-missing-field-initializers -fno-strict-aliasing -fPIC -fstack-protector -fno-common -Wformat -Wformat-security -I/spdk/include
// #cgo LDFLAGS: -pthread -Wl,-z,relro,-z,now -Wl,-z,noexecstack -L/spdk/build/lib -Wl,--whole-archive -lspdk_log -lspdk_nvme -lspdk_sock -lspdk_sock_posix -lspdk_thread -lspdk_vmd -lspdk_util -Wl,--no-whole-archive /spdk/build/lib/libspdk_env_dpdk.a -Wl,--whole-archive /spdk/dpdk/build/lib/librte_eal.a /spdk/dpdk/build/lib/librte_mempool.a /spdk/dpdk/build/lib/librte_ring.a /spdk/dpdk/build/lib/librte_mbuf.a /spdk/dpdk/build/lib/librte_mempool_ring.a /spdk/dpdk/build/lib/librte_pci.a /spdk/dpdk/build/lib/librte_bus_pci.a /spdk/dpdk/build/lib/librte_kvargs.a /spdk/dpdk/build/lib/librte_vhost.a /spdk/dpdk/build/lib/librte_net.a /spdk/dpdk/build/lib/librte_hash.a /spdk/dpdk/build/lib/librte_cryptodev.a -Wl,--no-whole-archive -lnuma -ldl -luuid
// #include <stdlib.h>
// #include "spdk_file.h"
import "C"
import (
	"container/list"
	"errors"
	"fmt"
	"os"
	"strings"
	"unsafe"
)

type SpdkFile struct {
	proto   string
	devAddr string
	// This should be treated as an opaque field, but is required to live for the
	// lifetime of the database.
	ctx    C.struct_SpdkCtx
	queued *list.List
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

	// Small amount of sanity testing here for kicks.
	f.WriteAt([]byte(strings.Repeat("a", 4096)), 0)
	f.Sync()
	d := make([]byte, 4096)
	f.ReadAt(d, 0)
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
	for pending := f.queued.Len(); pending > 0; pending = f.queued.Len() {
		done := int(C.ProcessCompletions(&f.ctx, C.uint(pending)))
		// Remove requests that were completed and update the file size if needed.
		for i := 0; i < done; i++ {
			first := f.queued.Front()
			iou := first.Value.(*C.struct_Iou)
			C.spdk_free(unsafe.Pointer(iou.buf))
			f.queued.Remove(first)
		}
	}
	return nil
}

func (f *SpdkFile) Fd() uintptr {
	return 0
}

func (f *SpdkFile) Truncate(size int64) error {
	return errors.New("Not implemented")
}

func (f *SpdkFile) Name() string {
	return "spdk-file"
}

func (f *SpdkFile) WriteAt(b []byte, off int64) (int, error) {
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

func (f *SpdkFile) Stat() (os.FileInfo, error) {
	return nil, errors.New("Not implemented")
}

func (f *SpdkFile) ReadAt(b []byte, off int64) (int, error) {
	size := len(b)
	var err error

	iou, err2 := f.initIou(C.SpdkRead, off, size)
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
	err = f.waitForRead(iou, b)
	if err != nil {
		return -1, err
	}

	return size, err
}

func (f *SpdkFile) Close() error {
	C.SpdkTeardown(&f.ctx)
	return nil
}

func (f *SpdkFile) Read(b []byte) (int, error) {
	return -1, errors.New("Not implemented")
}

func (f *SpdkFile) Write(b []byte) (int, error) {
	return -1, errors.New("Not implemented")
}

func (f *SpdkFile) Seek(offset int64, whence int) (int64, error) {
	return -1, errors.New("Not implemented")
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

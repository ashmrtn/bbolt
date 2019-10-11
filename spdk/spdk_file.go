package spdk

// #cgo CFLAGS: -g -O2 -Wall -Wextra -Wno-unused-parameter -Wno-missing-field-initializers -fno-strict-aliasing -fPIC -fstack-protector -fno-common -Wformat -Wformat-security -I/spdk/include
// #cgo LDFLAGS: -pthread -Wl,-z,relro,-z,now -Wl,-z,noexecstack -L/spdk/build/lib -Wl,--whole-archive -lspdk_log -lspdk_nvme -lspdk_sock -lspdk_sock_posix -lspdk_thread -lspdk_vmd -lspdk_util -Wl,--no-whole-archive /spdk/build/lib/libspdk_env_dpdk.a -Wl,--whole-archive /spdk/dpdk/build/lib/librte_eal.a /spdk/dpdk/build/lib/librte_mempool.a /spdk/dpdk/build/lib/librte_ring.a /spdk/dpdk/build/lib/librte_mbuf.a /spdk/dpdk/build/lib/librte_mempool_ring.a /spdk/dpdk/build/lib/librte_pci.a /spdk/dpdk/build/lib/librte_bus_pci.a /spdk/dpdk/build/lib/librte_kvargs.a /spdk/dpdk/build/lib/librte_vhost.a /spdk/dpdk/build/lib/librte_net.a /spdk/dpdk/build/lib/librte_hash.a /spdk/dpdk/build/lib/librte_cryptodev.a -Wl,--no-whole-archive -lnuma -ldl -luuid
// #include <stdlib.h>
// #include "spdk_file.h"
import "C"
import (
	"errors"
	"os"
	"strings"
	"unsafe"
)

type SpdkFile struct {
	proto   string
	devAddr string
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
	res := C.SpdkInit(trid)
	if res != 0 {
		return nil, errors.New("Unable to initialize spdk")
	}
	// TODO(ashmrtnz): Uncomment when actually working.
	//return f, nil
	return nil, errors.New("Not implemented")
}

func (f *SpdkFile) Sync() error {
	return errors.New("Not implemented")
}

func (f *SpdkFile) Fd() uintptr {
	return 0
}

func (f *SpdkFile) Truncate(size int64) error {
	return errors.New("Not implemented")
}

func (f *SpdkFile) Name() string {
	return ""
}

func (f *SpdkFile) WriteAt(b []byte, off int64) (int, error) {
	return -1, errors.New("Not implemented")
}

func (f *SpdkFile) Stat() (os.FileInfo, error) {
	return nil, errors.New("Not implemented")
}

func (f *SpdkFile) ReadAt(b []byte, off int64) (int, error) {
	return -1, errors.New("Not implemented")
}

func (f *SpdkFile) Close() error {
	return errors.New("Not implemented")
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

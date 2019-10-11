#include "spdk_file.h"

#include <stdio.h>

#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/nvme.h"

// Trid should be in the form "trtype=<type> traddr=<addr>"
int SpdkInit(const char *tridStr) {
  struct spdk_env_opts opts;
  struct spdk_nvme_transport_id trid;
  struct spdk_pci_addr pci_addr;

  spdk_env_opts_init(&opts);
  opts.name = "bbolt-spdk";
  opts.shm_id = 0;
  if (spdk_env_init(&opts) < 0) {
    SPDK_ERRLOG("Unable to initialize spdk environment\n");
    return -1;
  }

  memset(&trid, 0, sizeof(trid));
  trid.trtype = SPDK_NVME_TRANSPORT_PCIE;
  if (spdk_nvme_transport_id_parse(&trid, tridStr) < 0) {
    SPDK_ERRLOG("Failed to parse transport type and device %s\n", tridStr);
    return -1;
  }
  if (trid.trtype != SPDK_NVME_TRANSPORT_PCIE) {
    SPDK_ERRLOG("Unsupported transport type and device %s\n", tridStr);
    return -1;
  }
  if (spdk_pci_addr_parse(&pci_addr, trid.traddr) < 0) {
    SPDK_ERRLOG("Invalid device address %s\n", trid.traddr);
    return -1;
  }
  spdk_pci_addr_fmt(trid.traddr, sizeof(trid.traddr), &pci_addr);

  return 0;
}

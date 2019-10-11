#include "spdk_file.h"

#include <stdio.h>

#include "spdk/env.h"
#include "spdk/log.h"

bool probeCb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr_opts *opts);
void attachCb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts);
//void opCb(void *cb_ctx, const struct spdk_nvme_cpl *cpl);

// Trid should be in the form "trtype=<type> traddr=<addr>"
int SpdkInit(const char *tridStr, struct SpdkCtx *ctx) {
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

  // Probe devices until we find the one we want to attach to.
  // Assuming we haven't already probed, so just probe everything here.
  if (spdk_nvme_probe(&trid, ctx, probeCb, attachCb, NULL) != 0) {
    SPDK_ERRLOG("spdk_nvme_probe() failed\n");
    return -1;
  }

  return 0;
}

void SpdkTeardown(struct SpdkCtx *ctx) {
  if (ctx->qpair != NULL) {
    spdk_nvme_ctrlr_free_io_qpair(ctx->qpair);
  }
  if (ctx->ctrlr != NULL) {
    spdk_nvme_detach(ctx->ctrlr);
  }
}

bool probeCb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr_opts *opts) {
  // Always say that we would like to attach to the controller since we aren't
  // really looking for anything specific.
  return true;
}

void attachCb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts) {
  struct spdk_nvme_io_qpair_opts qpopts;
  struct SpdkCtx *ctx = (struct SpdkCtx*) cb_ctx;

  if (ctx->qpair != NULL) {
    SPDK_ERRLOG("Already attached to a qpair\n");
    return;
  }

  ctx->ctrlr_opts = *opts;
  ctx->ctrlr = ctrlr;
  ctx->tr_id = *trid;

  ctx->ns = spdk_nvme_ctrlr_get_ns(ctx->ctrlr, 1);

  if (ctx->ns == NULL) {
    SPDK_ERRLOG("Can't get namespace by id %d\n", 1);
    return;
  }

  if (!spdk_nvme_ns_is_active(ctx->ns)) {
    SPDK_ERRLOG("Inactive namespace at id %d\n", 1);
    return;
  }

  spdk_nvme_ctrlr_get_default_io_qpair_opts(ctx->ctrlr, &qpopts,
      sizeof(qpopts));
  qpopts.delay_pcie_doorbell = false;

  ctx->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctx->ctrlr, &qpopts,
      sizeof(qpopts));
  if (!ctx->qpair) {
    SPDK_ERRLOG("Unable to allocate nvme qpair\n");
    return;
  }
  ctx->namespaceSize = spdk_nvme_ns_get_size(ctx->ns);
  if (ctx->namespaceSize <= 0) {
    SPDK_ERRLOG("Unable to get namespace size for namespace %d\n", 1);
    return;
  }
}

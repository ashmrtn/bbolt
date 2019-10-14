#ifndef BBOLT_SPDK_SPDK_FILE_H_
#define BBOLT_SPDK_SPDK_FILE_H_

#include "spdk/nvme.h"

struct SpdkCtx {
  struct spdk_nvme_ctrlr *ctrlr;
  struct spdk_nvme_ctrlr_opts ctrlr_opts;
  struct spdk_nvme_transport_id tr_id;
  struct spdk_nvme_qpair *qpair;
  struct spdk_nvme_ns *ns;

  unsigned long SectorSize;
  unsigned long long NamespaceSize;
};

enum IoType {
  SpdkWrite,
  SpdkRead
};

struct Iou {
  enum IoType ioType;
  unsigned long int bufSize;
  unsigned long long int offset;
  unsigned long long lba;
  unsigned long lbaCount;
  char *buf;
};

// Trid should be in the form "trtype=<type> traddr=<addr>"
int SpdkInit(const char *tridStr, struct SpdkCtx *ctx);
void SpdkTeardown(struct SpdkCtx *ctx);
int QueueIO(struct SpdkCtx *ctx, struct Iou *iou, char *data);

#endif  // BBOLT_SPDK_SPDK_FILE_H_

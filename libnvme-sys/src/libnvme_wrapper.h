#pragma once
#include "libnvme.h"

int nvme_zns_report_zones_wrapper(int fd, __u32 nsid, __u64 slba,
				  enum nvme_zns_report_options opts,
				  bool extended, bool partial,
				  __u32 data_len, void *data,
				  __u32 timeout, __u32 *result);

int nvme_identify_ns_wrapper(int fd, __u32 nsid, struct nvme_id_ns *ns);

int nvme_zns_identify_ns_wrapper(int fd, __u32 nsid,
			struct nvme_zns_id_ns *data);

int nvme_zns_identify_ctrl_wrapper(int fd, struct nvme_zns_id_ctrl *id);
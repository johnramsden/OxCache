#!/bin/sh

# OUT_DIR=.build
# LIBNVME_SRC_DIR=external/libnvme/
RUSTFLAGS='-C target-feature=-crt-static' cargo build

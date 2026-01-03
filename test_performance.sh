#!/bin/bash
# Performance test script for stargz vs regular tar

set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 <layer.tar>"
    exit 1
fi

LAYER_TAR="$1"
STARGZ_FILE="${LAYER_TAR%.tar}.stargz"

echo "Converting $LAYER_TAR to stargz format..."
time cargo run --release -- convert "$LAYER_TAR" "$STARGZ_FILE"

echo -e "\nComparing file sizes:"
ls -lh "$LAYER_TAR" "$STARGZ_FILE"

echo -e "\nTesting read performance:"
echo "Regular tar:"
time tar -tf "$LAYER_TAR" > /dev/null

echo -e "\nStargz:"
time cargo run --release -- read "$STARGZ_FILE" > /dev/null

echo -e "\nTesting random access (first 10 files):"
cargo run --release -- read "$STARGZ_FILE" | head -10
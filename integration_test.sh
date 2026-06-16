#!/bin/bash
set -e

# CI Pipeline Remote Cache Integration Test
# This script demonstrates the full remote cache workflow

echo "=========================================="
echo "CI Pipeline Remote Cache Integration Test"
echo "=========================================="
echo ""

# Configuration
SERVER_PORT=9876
SERVER_URL="http://localhost:$SERVER_PORT"
TEST_DIR="/tmp/ci-pipeline-test"
STORAGE_DIR="/tmp/ci-cache-storage"
PIPELINE_FILE="test_remote_cache.yml"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    if [ -n "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
    rm -rf "$TEST_DIR"
    rm -rf "$STORAGE_DIR"
    echo "Cleanup complete."
}
trap cleanup EXIT

# Build the project
echo -e "${YELLOW}Step 1: Building project...${NC}"
cargo build --bin ci-pipeline --bin ci-cache-server 2>&1
echo -e "${GREEN}✓ Build successful${NC}"
echo ""

# Setup test directory
echo -e "${YELLOW}Step 2: Setting up test environment...${NC}"
rm -rf "$TEST_DIR" "$STORAGE_DIR"
mkdir -p "$TEST_DIR"
mkdir -p "$STORAGE_DIR"

# Create server config
cat > /tmp/cache-server-config.toml <<EOF
listen_addr = "0.0.0.0:$SERVER_PORT"
storage_dir = "$STORAGE_DIR"
max_size_mb = 500
ttl_days = 7
EOF

# Copy test pipeline to test directory
cp "$PIPELINE_FILE" "$TEST_DIR/pipeline.yml"

echo -e "${GREEN}✓ Test environment ready${NC}"
echo ""

# Start the cache server
echo -e "${YELLOW}Step 3: Starting ci-cache-server...${NC}"
cargo run --bin ci-cache-server -- --config /tmp/cache-server-config.toml &
SERVER_PID=$!

# Wait for server to start
sleep 2
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo -e "${RED}✗ Failed to start cache server${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Cache server started (PID: $SERVER_PID)${NC}"
echo ""

# First run - cache miss
echo -e "${YELLOW}Step 4: First pipeline execution (cache miss → push to remote)...${NC}"
echo ""

cd "$TEST_DIR"
START_TIME=$(date +%s%N)
cargo run --manifest-path "$OLDPWD/Cargo.toml" --bin ci-pipeline -- run -f pipeline.yml 2>&1 | tee first_run.log
FIRST_EXIT=$?
END_TIME=$(date +%s%N)
FIRST_DURATION=$(( (END_TIME - START_TIME) / 1000000 ))

cd "$OLDPWD"

if [ $FIRST_EXIT -ne 0 ]; then
    echo -e "${RED}✗ First pipeline run failed${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✓ First run completed in ${FIRST_DURATION}ms${NC}"
echo ""

# Check that cache was pushed to remote
echo -e "${YELLOW}Step 5: Verifying cache was pushed to remote...${NC}"
echo ""
cargo run --manifest-path "$PWD/Cargo.toml" --bin ci-pipeline -- cache -f "$TEST_DIR/pipeline.yml" list 2>&1
echo ""

# Check storage directory
echo "Storage directory contents:"
find "$STORAGE_DIR" -type f | sort
echo ""

CACHE_COUNT=$(find "$STORAGE_DIR" -name "*.tar.gz" | wc -l)
if [ "$CACHE_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Found $CACHE_COUNT cache entries in remote storage${NC}"
else
    echo -e "${RED}✗ No cache entries found in remote storage${NC}"
    exit 1
fi
echo ""

# Clean local cache and artifacts
echo -e "${YELLOW}Step 6: Cleaning local cache (simulating fresh environment)...${NC}"
rm -rf "$TEST_DIR/.ci"
rm -rf "$TEST_DIR/build_output"
echo -e "${GREEN}✓ Local cache cleaned${NC}"
echo ""

# Second run - should hit remote cache
echo -e "${YELLOW}Step 7: Second pipeline execution (remote cache hit)...${NC}"
echo ""

cd "$TEST_DIR"
START_TIME=$(date +%s%N)
cargo run --manifest-path "$OLDPWD/Cargo.toml" --bin ci-pipeline -- run -f pipeline.yml 2>&1 | tee second_run.log
SECOND_EXIT=$?
END_TIME=$(date +%s%N)
SECOND_DURATION=$(( (END_TIME - START_TIME) / 1000000 ))

cd "$OLDPWD"

if [ $SECOND_EXIT -ne 0 ]; then
    echo -e "${RED}✗ Second pipeline run failed${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✓ Second run completed in ${SECOND_DURATION}ms${NC}"
echo ""

# Compare durations
echo -e "${YELLOW}Step 8: Comparing execution times...${NC}"
echo "  First run (cache miss):  ${FIRST_DURATION}ms"
echo "  Second run (cache hit):   ${SECOND_DURATION}ms"
echo ""

if [ $SECOND_DURATION -lt $FIRST_DURATION ]; then
    SPEEDUP=$(( FIRST_DURATION - SECOND_DURATION ))
    echo -e "${GREEN}✓ Second run was faster by ${SPEEDUP}ms - remote cache is working!${NC}"
else
    echo -e "${YELLOW}⚠ Second run was not faster (could be due to other factors)${NC}"
fi
echo ""

# Test cache stats
echo -e "${YELLOW}Step 9: Checking remote cache stats...${NC}"
echo ""
cargo run --manifest-path "$PWD/Cargo.toml" --bin ci-pipeline -- cache -f "$TEST_DIR/pipeline.yml" stats 2>&1
echo ""

# Test GC
echo -e "${YELLOW}Step 10: Testing garbage collection...${NC}"
echo ""
cargo run --manifest-path "$PWD/Cargo.toml" --bin ci-pipeline -- cache -f "$TEST_DIR/pipeline.yml" gc 2>&1
echo ""

# Test manual push/pull
echo -e "${YELLOW}Step 11: Testing manual cache push/pull...${NC}"
echo ""

# Create a test file
echo "Manual cache test content" > /tmp/test_cache_file.txt

# Push manually
cargo run --manifest-path "$PWD/Cargo.toml" --bin ci-pipeline -- cache -f "$TEST_DIR/pipeline.yml" push /tmp/test_cache_file.txt --key=manual-test-key 2>&1
echo ""

# Pull manually
cargo run --manifest-path "$PWD/Cargo.toml" --bin ci-pipeline -- cache -f "$TEST_DIR/pipeline.yml" pull manual-test-key --output=/tmp/pulled_cache_file.txt 2>&1
echo ""

# Verify content
if diff -q /tmp/test_cache_file.txt /tmp/pulled_cache_file.txt > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Manual push/pull test passed - files match${NC}"
else
    echo -e "${RED}✗ Manual push/pull test failed - files don't match${NC}"
    exit 1
fi
rm -f /tmp/test_cache_file.txt /tmp/pulled_cache_file.txt
echo ""

# Summary
echo "=========================================="
echo -e "${GREEN}Integration Test Complete - All tests passed!${NC}"
echo "=========================================="
echo ""
echo "Summary:"
echo "  ✓ Remote cache server started successfully"
echo "  ✓ Cache entries are pushed to remote storage"
echo "  ✓ Cache can be pulled from remote storage"
echo "  ✓ Cache stats endpoint works"
echo "  ✓ Garbage collection works"
echo "  ✓ Manual push/pull commands work"
echo ""

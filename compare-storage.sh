#!/bin/bash

echo "=============================================="
echo "Storage Performance Comparison"
echo "=============================================="
echo ""
echo "WARNING: Testing on /tmp (tmpfs/RAM) gives"
echo "misleading results for disk-based workloads!"
echo ""

echo "1. Testing /tmp (RAM filesystem)..."
echo "-------------------------------------------"
cd /tmp
fio --name=test-ram --rw=randwrite --bs=1k --size=10M --numjobs=1 --fsync=1 --runtime=30 2>/dev/null | grep -E "(IOPS=|fsync.*sync.*avg=)" | head -2

echo ""
echo "2. Testing actual disk (where ActiveMQ runs)..."
echo "-------------------------------------------"
cd /home/tmj/code/activemq
fio --name=test-disk --rw=randwrite --bs=1k --size=10M --numjobs=1 --fsync=1 --runtime=30 2>/dev/null | grep -E "(IOPS=|fsync.*sync.*avg=)" | head -2

echo ""
echo "=============================================="
echo "Filesystem Information:"
echo "=============================================="
echo "/tmp filesystem:"
df -T /tmp | tail -1
echo ""
echo "Current directory filesystem:"
df -T . | tail -1
echo ""
echo "NOTE: ActiveMQ performance will match the"
echo "ACTUAL DISK results, not the /tmp results!"
echo "=============================================="

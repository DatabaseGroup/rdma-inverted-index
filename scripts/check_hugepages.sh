#!/bin/bash

echo "=============================="
grep Huge /proc/meminfo
echo "------------------------------"
numastat -cm | grep "Node 1\|Huge"
echo "=============================="

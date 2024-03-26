#!/bin/bash

if [ "$#" -ne 1 ]
then
  echo "Usage: ./create_hugepages.sh <num-hugepages>"
  exit
fi

echo "create $1 hugepages of size 2MB on NUMA node 1"
echo $1 > /sys/devices/system/node/node1/hugepages/hugepages-2048kB/nr_hugepages

<h1 align="center">Scalable Distributed Inverted List Indexes in Disaggregated Memory</h1>

Implementation of distributed inverted list indexing techniques under memory disaggregation.
This is the source code of the paper "*Scalable Distributed Inverted List Index in Disaggregated Memory*" published and presented at SIGMOD'24.

<p align="center">
  <a href="#setup">Setup</a> •
  <a href="#usage">Usage</a> •
  <a href="#data-preprocessing">Data Preprocessing</a> •
  <a href="#quick-start">Quick Start</a>
</p>

---

### Citation
```
@article{10.1145/3654974,
    author = {Widmoser, Manuel and Kocher, Daniel and Augsten, Nikolaus},
    title = {Scalable Distributed Inverted List Indexes in Disaggregated Memory},
    journal = {Proc. ACM Manag. Data},
    publisher = {Association for Computing Machinery},
    year = {2024},
    volume = {2},
    number = {3},
    url = {https://doi.org/10.1145/3654974},
    doi = {10.1145/3654974}
}
```

## Setup

All our experiments were conducted on an 9-node cluster with five compute nodes and four memory nodes.
Each compute node has two Intel Xeon E5-2630 v3 2.40GHz processors with 16 cores (8 cores each; hyperthreading enabled) and the memory nodes have two physical Intel Xeon E5-2603 v4 1.70GHz processors with 12 cores (6 cores each).
All machines run Debian 10 Buster with a Linux 4.19 kernel, and are equipped with 96GB of main memory and a Mellanox ConnectX-3 NIC connected to a 18-port SX6018/U1 InfiniBand switch (FDR 56Gbps).

### C++ Libraries and Unix Packages

The following C++ libraries and Unix packages are required to compile the code.

* [ibverbs](https://github.com/linux-rdma/rdma-core/tree/master)
* [boost](https://www.boost.org/doc/libs/1_83_0/doc/html/program_options.html) (to support `boost::program_options` for
  CLI parsing)
* pthreads (for multithreading)
* [oneTBB](https://github.com/oneapi-src/oneTBB) (for concurrent data structures)
* a C++ compiler that supports C++17 (we used `clang++-12`)
* cmake
* numactl

### Mellanox OFED Packages

The following is the output of `ofed_info`, which shows OFED software version information (issued on the Linux InfiniBand nodes):

```
MLNX_OFED_LINUX-5.0-2.1.8.0 (OFED-5.0-2.1.8):
Installed Packages:
-------------------
ii  ar-mgr                                         1.0-0.49.MLNX20200216.g4ea049f.50218                             amd64        Adaptive Routing Manager
ii  cc-mgr                                         1.0-0.48.MLNX20200216.g4ea049f.50218                             amd64        Congestion Control Manager
ii  dapl2-utils                                    2.1.10.1.f1e05b7a-3                                              amd64        utilities for use with the DAPL libraries
ii  dump-pr                                        1.0-0.44.MLNX20200216.g4ea049f.50218                             amd64        Dump PathRecord Plugin
ii  hcoll                                          4.5.3045-1.50218                                                 amd64        Hierarchical collectives (HCOLL)
ii  ibacm                                          50mlnx1-1.50218                                                  amd64        InfiniBand Communication Manager Assistant (ACM)
ii  ibdump                                         6.0.0-1.50218                                                    amd64        Mellanox packets sniffer tool
ii  ibsim                                          0.9-1.50218                                                      amd64        InfiniBand fabric simulator for management
ii  ibsim-doc                                      0.9-1.50218                                                      all          documentation for ibsim
ii  ibutils                                        1.5.7.1-0.12.gdcaeae2.50218                                      amd64        InfiniBand network utilities
ii  ibutils2                                       2.1.1-0.121.MLNX20200324.g061a520.50218                          amd64        OpenIB Mellanox InfiniBand Diagnostic Tools
ii  ibverbs-providers:amd64                        50mlnx1-1.50218                                                  amd64        User space provider drivers for libibverbs
ii  ibverbs-utils                                  50mlnx1-1.50218                                                  amd64        Examples for the libibverbs library
ii  infiniband-diags                               50mlnx1-1.50218                                                  amd64        InfiniBand diagnostic programs
ii  iser-dkms                                      5.0-OFED.5.0.2.1.8.1.g5f67178                                    all          DKMS support fo iser kernel modules
ii  isert-dkms                                     5.0-OFED.5.0.2.1.8.1.g5f67178                                    all          DKMS support fo isert kernel modules
ii  kernel-mft-dkms                                4.14.0-105                                                       all          DKMS support for kernel-mft kernel modules
ii  knem                                           1.1.3.90mlnx1-OFED.5.0.0.3.8.1.g12569ca                          amd64        userspace tools for the KNEM kernel module
ii  knem-dkms                                      1.1.3.90mlnx1-OFED.5.0.0.3.8.1.g12569ca                          all          DKMS support for mlnx-ofed kernel modules
ii  libcaf-openmpi-3:amd64                         2.4.0-2                                                          amd64        Co-Array Fortran libraries for gfortran (OpenMPI)
ii  libcoarrays-openmpi-dev:amd64                  2.4.0-2                                                          amd64        Co-Array Fortran libraries for gfortran - development files (OpenMPI)
ii  libdapl-dev                                    2.1.10.1.f1e05b7a-3                                              amd64        development files for the DAPL libraries
ii  libdapl2                                       2.1.10.1.f1e05b7a-3                                              amd64        Direct Access Programming Library (DAPL)
ii  libibdm1                                       1.5.7.1-0.12.gdcaeae2.50218                                      amd64        InfiniBand network diagnostic library
ii  libibmad-dev:amd64                             50mlnx1-1.50218                                                  amd64        Development files for libibmad
ii  libibmad5:amd64                                50mlnx1-1.50218                                                  amd64        Infiniband Management Datagram (MAD) library
ii  libibnetdisc5:amd64                            50mlnx1-1.50218                                                  amd64        InfiniBand diagnostics library
ii  libibumad-dev:amd64                            50mlnx1-1.50218                                                  amd64        Development files for libibumad
ii  libibumad3:amd64                               50mlnx1-1.50218                                                  amd64        InfiniBand Userspace Management Datagram (uMAD) library
ii  libibverbs-dev:amd64                           50mlnx1-1.50218                                                  amd64        Development files for the libibverbs library
ii  libibverbs1:amd64                              50mlnx1-1.50218                                                  amd64        Library for direct userspace use of RDMA (InfiniBand/iWARP)
ii  libibverbs1-dbg:amd64                          50mlnx1-1.50218                                                  amd64        Debug symbols for the libibverbs library
ii  libopenmpi-dev:amd64                           3.1.3-11                                                         amd64        high performance message passing library -- header files
ii  libopenmpi3:amd64                              3.1.3-11                                                         amd64        high performance message passing library -- shared library
ii  libopensm                                      5.6.0.MLNX20200217.cedc1e4-0.1.50218                             amd64        Infiniband subnet manager libraries
ii  libopensm-devel                                5.6.0.MLNX20200217.cedc1e4-0.1.50218                             amd64        Developement files for OpenSM
ii  librdmacm-dev:amd64                            50mlnx1-1.50218                                                  amd64        Development files for the librdmacm library
ii  librdmacm1:amd64                               50mlnx1-1.50218                                                  amd64        Library for managing RDMA connections
ii  mlnx-ethtool                                   5.4-1.50218                                                      amd64        This utility allows querying and changing settings such as speed,
ii  mlnx-iproute2                                  5.4.0-1.50218                                                    amd64        This utility allows querying and changing settings such as speed,
ii  mlnx-ofed-kernel-dkms                          5.0-OFED.5.0.2.1.8.1.g5f67178                                    all          DKMS support for mlnx-ofed kernel modules
ii  mlnx-ofed-kernel-utils                         5.0-OFED.5.0.2.1.8.1.g5f67178                                    amd64        Userspace tools to restart and tune mlnx-ofed kernel modules
ii  mlnx-rdma-rxe-dkms                             5.0-OFED.5.0.2.1.8.1.g5f67178                                    all          DKMS support for rdma-rxe kernel modules
ii  mpitests                                       3.2.20-e1a0676.50218                                             amd64        Set of popular MPI benchmarks and tools IMB 2018 OSU benchmarks ver 4.0.1 mpiP-3.3 IPM-2.0.6
ii  mstflint                                       4.13.0-1.41.g4e8819c.50218                                       amd64        Mellanox firmware burning application
ii  openmpi                                        4.0.3rc4-1.50218                                                 all          Open MPI
ii  openmpi-bin                                    3.1.3-11                                                         amd64        high performance message passing library -- binaries
ii  openmpi-common                                 3.1.3-11                                                         all          high performance message passing library -- common files
ii  opensm                                         5.6.0.MLNX20200217.cedc1e4-0.1.50218                             amd64        An Infiniband subnet manager
ii  opensm-doc                                     5.6.0.MLNX20200217.cedc1e4-0.1.50218                             amd64        Documentation for opensm
ii  perftest                                       4.4+0.5-1                                                        amd64        Infiniband verbs performance tests
ii  rdma-core                                      50mlnx1-1.50218                                                  amd64        RDMA core userspace infrastructure and documentation
ii  rdmacm-utils                                   50mlnx1-1.50218                                                  amd64        Examples for the librdmacm library
ii  rshim-dkms                                     1.18-0.gb99e894.50218                                            all          DKMS support fo rshim kernel modules
ii  sharp                                          2.1.0.MLNX20200223.f63394a9c8-1.50218                            amd64        SHArP switch collectives
ii  srp-dkms                                       5.0-OFED.5.0.2.1.8.1.g5f67178                                    all          DKMS support fo srp kernel modules
ii  srptools                                       50mlnx1-1.50218                                                  amd64        Tools for Infiniband attached storage (SRP)
ii  ucx                                            1.8.0-1.50218                                                    amd64        Unified Communication X
```

### Nodes

Adjust the IP addresses of the cluster nodes accordingly in `rdma-library/library/utils.cc`:
https://github.com/DatabaseGroup/rdma-inverted-index/blob/8f251cb1422e33773c42e6d392d3f3d29ed19b74/rdma-library/library/utils.cc#L11-L22

### Compilation

After cloning the repository and installing the requirements, the code must be compiled on all cluster nodes:

```
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang++ ..
make
```

## Usage

The following index executables exist: `term_index` (read-only term-based index), `document_index` (read-only
document-based index), `block_index` (read-only block-based index), `dynamic_block_index` (block-based index that
supports updates).
A cluster node can either be a memory node or a compute node, exact one compute node must be an initiator.

To reduce the number of address translations, it is recommended to allocate hugepages on all the cluster nodes:

```bash
echo n > /sys/devices/system/node/node1/hugepages/hugepages-2048kB/nr_hugepages
```

where `n` is the number of hugepages.
To run the index without hugepages, use `-DNOHUGEPAGES` as an additional compiler flag.

### Compute Nodes

Run on the initiator:

```
numactl --membind=1 <executable> --initiator --index-dir <index-binary-directory> --query-file <query-file> --servers <memory-nodes> --clients <compute-nodes> --threads <num-threads> --operation <operation> --block-size <block-size>
```

* `<executable>` is either `term_index`, `document_index`, `block_index`, or `dynamic_block_index`
* `<index-binary-directory>` is the directory where the index binary files are stored (cf. Data Preprocessing below)
* `<query-file>` is the file that contains the queries (cf. [Data Preprocessing](#data-preprocessing) below)
* `<memory-nodes>` is a list of memory nodes (separated with white-spaces)
* `<compute-nodes>` is a list of compute nodes (excluding the initiator)
* `<num-threads` is the number of compute threads per compute node
* `<operation>` is the operation performed for read queries: either `intersection` or `union`
* `<block-size>` is the size of a block (relevant only for `block_index` and `dynamic_block_index`)

Run on the remaining compute nodes:

```
numactl --membind=1 <executable> --servers <memory-nodes>
```

### Memory Nodes

```
numactl --membind=1 --cpunodebind=1 <executable> --is-server --num-clients <num-compute-nodes>
```

* `<num-compute-nodes>` is the number of compute nodes that will connect to the memory node

### Synopsis

The following CLI options can be adjusted:

```
Allowed options:
  -h [ --help ]                    Show help message
  -s [ --is-server ]               Program acts as server if set
  --servers arg                    A list of server nodes to which a client
                                   connects, e.g., "cluster3"
  --clients arg                    A list of client nodes to which the
                                   initiator connects, e.g., "cluster4
                                   cluster5"
  -i [ --initiator ]               Program acts as initiating client if set
  -c [ --num-clients ] arg (=1)    Number of clients that connect to each
                                   server (relevant only for server nodes)
  --port arg (=1234)               TCP port
  --ib-port arg (=1)               Port of infiniband device
  --max-poll-cqes arg (=16)        Number of outstanding RDMA operations
                                   allowed (hardware-specific)
  --max-send-wrs arg (=1024)       Maximum number of outstanding send work
                                   requests
  --max-receive-wrs arg (=1024)    Maximum number of outstanding receive work
                                   requests
  -d [ --index-dir ] arg           Location of the partitioned index files.
  -q [ --query-file ] arg          Input file containing queries for the index.
  -t [ --threads ] arg             Number of threads per compute node
  -o [ --operation ] arg           Operation performed on lists: either
                                   "intersection" or "union".
  -p [ --disable-thread-pinning ]  Disables pinning compute threads to physical
                                   cores if set.
  -b [ --block-size ] arg (=1024)  Block size in bytes (only used by
                                   [dynamic_]block_index).
```

## Data Preprocessing

### Datasets

We have used the following datasets in our experiments:

#### TWITTER

Download and extract the dataset from https://twitter.mpi-sws.org/ and run

```bash
python3 scripts/twitter/extract.py links-anon.txt > twitter-lists.txt
python3 scripts/twitter/reassign_ids.py twitter-lists.txt > twitter-lists-reassigned.txt
```

to create the index file. For further binary processing (see below), manually add the universe size and the number of
lists to the top of the file.
The queries are generated using the `create_popular_queries.py` script.

⏰ Since processing TWITTER takes a while, we provide the preprocessed [binary index file](#creating-binary-index-files) and the corresponding queries [here](https://frosch.cosy.sbg.ac.at/datasets/sets/twitter-mpi).

#### SSB

Use the [SSB-DB generator](https://github.com/vadimtk/ssb-dbgen) (`dbgen -s 1 -T a`) to create the tables and store
them in a directory called `tables`, then run

```bash
python3 scripts/ssb/ssb.py > ssb-lists.txt
```

to create the index file.
The queries are generated using

```bash
python3 scripts/ssb/generate_ssb_queries.py <num-queries> > ssb-queries.txt
```

#### CCNEWS

The CCNEWS data can be downloaded from https://doi.org/10.48610/1dcb974 (the compressed `.ciff`
file). With the [ciff tool](https://github.com/osirrc/ciff), the lists can be extracted.
The queries are given and must be converted.

⏰ Since processing CCNEWS takes a while, we provide the preprocessed [binary index file](#creating-binary-index-files) and the corresponding queries [here](https://frosch.cosy.sbg.ac.at/datasets/sets/ccnews).

#### TOY

We have used

```bash
python3 scripts/uniform.py 2000000 100000 50 100 200
```

to create 2M random documents containing 100 terms on average, the `scripts/index_from_documents.py` script to convert the documents to lists, and

```bash
python3 scripts/uniform.py 100000 100000 2 5 10
```

to create uniform random queries with an average size of 5.
The documents must be converted to lists, and the queries prefixed with `r:` (see below).

### Creating Binary Index Files

To create a binary index file, the dataset file should have the following *input format*:

```
universe size
number of lists (is the same in many cases)
list id: list entries separated via whitespace
...
```

List ids should be consecutive. For instance:

```
29497691
29497691
0: 345422 651571 915383 
1: 494792 537875 1066751 1095655 1358056
...
```

Using `data_processing/serializer -i <input-file> -o <output-file>`, we get the following *output format* (as binary
output, all 32bit
integers):

``` 
<universe-size><numer-of-lists><list-id><list-size><list-entry-1>...
```

⏰ For CCNEWS and TWITTER, we provide the binary files since processing them takes quite a while.
SSB and TOY can be easily reproduced by following the steps from [above](#datasets).

### Partitioning Index Binary Files

Finally, we must partition the index upfront such that our algorithms can deal with it (they have to simply load the
partitioned binary index and meta files rather than partition the index each time on its own):

```
data_processing/partitioner -i <binary-input-file> -o <output-path> -s <strategy> -n <num-nodes> [-b <block-size>] [-a -q <query-file>]
```

* `<binary-input-file>` is the output of `serializer`, i.e., the entire serialized binary index.
* `<output-path>` is a path to which the partitioned index binary files are written.
* `<strategy>` is the partitioning strategy, can either be `term`, `document`, or `block`.
* `<num-nodes>` is the number of memory nodes, i.e., the number of partitions.
* `<block-size>` is the block size in bytes (used only if the strategy is `block`, default is 2048).
* `-a` partitions only accessed lists (given in the query file `<query-file>`), currently only implemented for
  block-based

The output directory (including its files) must be accessible by all compute (for meta data) and memory nodes (for index
data), e.g., stored on a network file storage.

### Query Files

The content of a query file must be as follows:

```
r: <term_1> ... <term_n>
i: <doc-id> <term>
```

* `r:` indicates a read query (computes the intersection between the lists given by the terms)
* `i:` indicates an insert query (inserts the document id to the list represented by the term)

### Constructing Update Queries

For inserts, we create 95% of the index and use the remaining 5% for index queries (drawn at random).
First, re-create the documents out of the binary index file by using `./create_documents > <doc_file>`.
Then with `./draw_documents_and_create_index`, we randomly draw 5% of the documents, store them in a separate file, and
build a 95% binary index.
Finally, the script `mix_queries.py` mixes read and insert queries.
With `split_inserts.py`, we can split the long insert queries into multiple single-term queries.
Please note that `create_documents.cc` and `draw_documents_and_create_index.cc` must be adjusted, respectively (TODO: CLI options):

https://github.com/DatabaseGroup/rdma-inverted-index/blob/8f251cb1422e33773c42e6d392d3f3d29ed19b74/src/data_processing/update_queries/create_documents.cc#L16-L17

https://github.com/DatabaseGroup/rdma-inverted-index/blob/8f251cb1422e33773c42e6d392d3f3d29ed19b74/src/data_processing/update_queries/draw_documents_and_create_index.cc#L19-L21


# Quick Start

The following assumptions are made for a quick test to run the code on two machines (a compute node and a memory node):
* both machines are within the same InfiniBand network,
* the [required packages](#setup) are installed on both machines,
* `clang++` is installed as C++ compiler,
* the IP addresses are adjusted accordingly (cf. [Nodes](#nodes)),
* `cluster1` is the compute node, `cluster2` the memory node,
* the directory `/mnt/dbgroup-share/example/` is accessible from both nodes (e.g., via NFS) and contains the files in `example/` from this repository (an exemplary binary index file and some random queries).


Clone and compile the code on **both** machines (without hugepage support):
```
git clone https://github.com/DatabaseGroup/rdma-inverted-index.git
cd rdma-inverted-index
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_CXX_FLAGS="-DNOHUGEPAGES" ..
make
```


On one of the machines, partition the dataset upfront:
```
data_processing/partitioner -i /mnt/dbgroup-share/example/example-lists.dat -o /mnt/dbgroup-share/example/ -s block -n 1 -b 1024
```

On the **memory node**, run:
```
numactl --membind=1 --cpunodebind=1 ./block_index --is-server --num-clients 1
```

On the **compute node**, run:
```
numactl --membind=1 ./block_index --initiator --index-dir /mnt/dbgroup-share/example/ --query-file /mnt/dbgroup-share/example/example-queries.txt --servers cluster2 --threads 4 --operation intersection --block-size 1024
```

The output on the **compute node** should be similar to
```
============================================================
                        CLIENT
============================================================
connect to:                   [cluster2]
is initiator:                 true
TCP port:                     1234
IB port:                      1
max outstanding CQEs:         16
max send work requests:       1024
max receive work requests:    1024
============================================================
index directory:              /mnt/dbgroup-share/example/
query file:                   /mnt/dbgroup-share/example/example-queries.txt
operation:                    intersection
number of threads:            4
threads pinned:               true
block size:                   1024
============================================================
1 device(s) found
Selected device: mlx4_0
num_cores: 16
physical cores per socket: 8
hyperthreading disabled
interleaved policy
client id: 0
number of clients: 1
connect to server cluster2
pairing: 533 -- 534
[STATUS]: pinned main thread to core 8
[STATUS]: exchange information with compute nodes
[STATUS]: receive access tokens of remote memory regions
[STATUS]: read meta data and assign remote pointers
[STATUS]: read queries
size of queries: 24132 Bytes
[STATUS]: allocate worker threads and read buffers
allocated ALIGNED MEM (no hugepage) at 140104859615296 with buffer size 262144
1 device(s) found
Selected device: mlx4_0
pairing: 534 -- 535
1 device(s) found
Selected device: mlx4_0
pairing: 535 -- 536
1 device(s) found
Selected device: mlx4_0
pairing: 536 -- 537
1 device(s) found
Selected device: mlx4_0
pairing: 537 -- 538
[STATUS]: run worker threads
[STATUS]: pinned thread 1 to core 0
[STATUS]: pinned thread 2 to core 9
[STATUS]: pinned thread 3 to core 1
query 0 [read] (len=2): [9 51]
query 100 [read] (len=2): [536 683]
query 200 [read] (len=3): [238 276 990]
query 300 [read] (len=4): [68 124 707 791]
query 400 [read] (len=4): [346 405 607 899]
query 500 [read] (len=5): [90 187 425 575 739]
query 600 [read] (len=5): [538 849 851 883 932]
query 700 [read] (len=6): [182 439 664 875 878 931]
query 800 [read] (len=7): [51 276 320 340 341 423 552]
query 900 [read] (len=8): [92 112 240 371 537 552 700 788]
[STATUS]: join compute threads
t0 processed queries: 337, READ lists: 0.387253, polling: 0, operation: 0
t1 processed queries: 245, READ lists: 0.475905, polling: 0, operation: 0
t2 processed queries: 257, READ lists: 0.374502, polling: 0, operation: 0
t3 processed queries: 161, READ lists: 0.406743, polling: 0, operation: 0
[STATUS]: gather query statistics
[STATUS]: gather timings

statistics:
{
  "allocated_read_buffers_size": 274432,
  "catalog_size": 8000,
  "mb_per_sec": 2907.1562137754227,
  "meta": {
    "algorithm": "block-based",
    "block_size": 1024,
    "compute_nodes": 1,
    "compute_threads": 4,
    "hyperthreading": "false",
    "index_directory": "example",
    "memory_nodes": 1,
    "operation": "intersection",
    "query_file": "example-queries.txt",
    "threads_pinned": "true"
  },
  "num_insert_queries": 0,
  "num_queries": 1000,
  "num_read_queries": 1000,
  "num_result": 0,
  "queries_per_sec": 564081,
  "rdma_reads_in_bytes": 5153792,
  "timings": {
    "query_c0": 1.772795,
    "query_total": 1.772795
  },
  "total_index_buffer_size": 1024000,
  "total_initial_index_size": 1024000,
  "universe_size": 1000
}
```

The output on the **memory node** should be similar to
```
============================================================
                        SERVER
============================================================
num clients:                  1
TCP port:                     1234
IB port:                      1
max outstanding CQEs:         16
max send work requests:       1024
max receive work requests:    1024
============================================================
1 device(s) found
Selected device: mlx4_0
num_cores: 32
physical cores per socket: 8
hyperthreading enabled
interleaved policy
pairing: 534 -- 533
[STATUS]: pinned main thread to core 8
[STATUS]: receive index file location
index file: /mnt/dbgroup-share/example/block1024_m1_of1_index.dat
index file size: 1024000
allocated ALIGNED MEM (no hugepage) at 140400725594176 with buffer size 1024000
[STATUS]: read index into memory
index size: 1024000
total index buffer size: 1024000
[STATUS]: register memory and distribute access token
[STATUS]: connect QPs of compute threads
pairing: 535 -- 534
pairing: 536 -- 535
pairing: 537 -- 536
pairing: 538 -- 537
[STATUS]: idle
{"allocate_index_buffer":0.023072,"read_file":0.675024,"read_index_into_memory":0.685865}
```
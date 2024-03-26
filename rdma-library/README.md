# RDMA Library

"High-level" library to connect machines, connect queue pairs, register memory regions, post RDMA verbs, etc.
The goal of this library is to conveniently wrap
the [ibverbs library](https://github.com/linux-rdma/rdma-core/tree/master/libibverbs).

[TODO: public library interface and namespaces...]

## Required C++ Libraries

* ibverbs
* Boost (for CLI parsing)
* pthreads (for multithreading)
* oneTBB (for concurrent data structures)

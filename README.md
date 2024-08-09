# Balancing Skewed Data Across Multiple SSDs
This project aims to provide a strategy to balance data blocks across multiple SSDs so that parallel access to them will be done in the most efficient way. 
Because of data skew, some blocks have higher probability to be accessed, while the other have lower. A simple idea is to put "hotter" blocks on separate devices so that they can be accessed in parallel. 
We want to elaborate on this and search for more advanced strategies.

## Project contributors
The study was conducted as part of a summer internship under the Summer@EPFL programme. It was held by intern Kseniya Shastakova supervised by Hamish Nicholson and Antonio Boffa at Data-Intensive Applications and Systems laboratory (DIAS). 

## Project description
**Problem**. Single server data-intensive systems frequently use multiple drives for storing data. Typically, through the OS-provided filesystem abstraction on top of either OS-provided software RAID (e.g. Linux LVM/mdadm) or a hardware RAID card. Both hardware and software RAID take multiple block devices (e.g. SSDs or HDDs) and provide the abstraction of a single logical block device that the file system can use. When a user writes a block with the filesystem `write` call, the filesystem then writes the block to the logical block device. The RAID system must determine which drive to write the block to. Existing RAID systems are workload oblivious; they use a fixed policy. However, data-intensive systems frequently have both highly skewed or correlated accesses. This means that the default drive allocation policy of the RAID system may be suboptimal depending on the data and query distribution. With workload knowledge, we may be able to better utilize the drives to achieve higher performance.  


**Project**. In this project, we will assume the use case of a relational database using a columnar format with late materialization. We are mostly considering the impact of data distribution skew in terms of the number of blocks accessed by a scan-filter-aggregate query like the following:

```
Select Sum(B) from my_table where A < X
```
(X is a placeholder value) In this query, every value of column A needs to be loaded from disk to check the predicate A < X. With late materialization, we only need to load the blocks of column B where at least one tuple passed the predicate. For example, if the data of A is uniformly distributed in the range [0, 99], and each of our blocks holds 10 values then: if our predicate is  A < 5 then the probability of needing to load a particular block of B is $1 - (1 - 0.05)^10 ~= 0.4$. A uniform distribution is the worst-case because, typically, block sizes are hundreds of KiB or a few MiB, so the probability of at least one value passing the predicate is very high. We would like to build a small model in python that takes as parameters the block size, the data distribution, and the predicate to evaluate the expected number of blocks of column B that must be loaded from disk. The goal is to guide us toward what kinds of data distributions and queries result in skewed access to a subset of the blocks in the column. This subset of blocks would be the hot set that needs to be carefully balanced across the available drives to maximize query performance. We can also use this information to estimate the performance of worst and best-case distribution of the hotset across multiple drives.

Once we have a preliminary idea of the expected performance, we will begin designing and implementing a storage system in C++ to demonstrate the simulated results in a concrete system. 


## Project structure
The project had the following steps:
* **Modeling**. We used a simple Python model to estimate the number of blocks that will be loaded with late matherialization. The results of this step are in `preliminary/` folder.
* **Implementing storage engine**. We designed and implemented a storage engine in C++ for evaluating the performance of different data placement strategies. `storage-engine/` folder contains a prototype of this storage-engine. The full version is kept private because it is implemented as a part of [Proteus](https://proteusdb.com/), which is a closed-source database developed by DIAS at the moment (08/09/2024).
* **Benchmarking**. We collected benchmark results using previously introduced storage engine. Benchmark results are in `benchmark_results/` folder.

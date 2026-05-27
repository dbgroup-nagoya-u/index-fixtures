# Index Fixtures

This repository is a collection of fixtures for testing index implementations.

## Build Options

- `DBGROUP_TEST_DISABLE_RECORD_MERGING`: Make Write/Upsert/Update operations overwrite records (default `ON`).
- `DBGROUP_TEST_DISABLE_SCAN_VERIFIER_TEST`: Disable scan verification (avoiding phantom read) tests (default `ON`).
- `DBGROUP_TEST_THREAD_NUM`: The maximum number of threads to perform unit tests (default `2`).
- `DBGROUP_TEST_EXEC_NUM`: The number of executions per a thread (default `1E5`).
- `DBGROUP_TEST_MAX_VARLEN_DATA_SIZE`: The expected maximum size of a variable-length data (default `32`).
- `DBGROUP_TEST_RANDOM_SEED`: A fixed seed value to reproduce unit tests (default `0`).
- `DBGROUP_TEST_OVERRIDE_MIMALLOC`: Override entire memory allocation with mimalloc (default `OFF`).

### Additional Build Options for Distributed Indexes

- `DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_NUM`: The number of servers in a cluster (default `1`).
- `DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_ID`: The ID of this server in a cluster (default `0`).

## Usage

...WIP (some sample files are in a `test` directory).

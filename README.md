# Index Fixtures

This repository is a collection of fixtures for testing index implementations.

## Build Options

- `DBGROUP_TEST_DISABLE_<OPS>_TEST`: Disable unit tests for a specified operation (default `OFF`).
    - Target operations are `READ`, `SCAN`, `SCAN_VERIFIER`, `WRITE`, `UPSERT`, `INSERT`, `UPDATE`, `DELETE`, and `BULKLOAD`.
- `DBGROUP_TEST_DISABLE_RECORD_MERGING`: Make Write/Upsert/Update operations overwrite records (default `OFF`).
- `DBGROUP_TEST_THREAD_NUM`: The maximum number of threads to perform unit tests (default `2`).
- `DBGROUP_TEST_EXEC_NUM`: The number of executions per a thread (default `1E5`).
- `DBGROUP_TEST_RANDOM_SEED`: A fixed seed value to reproduce unit tests (default `0`).
- `DBGROUP_TEST_OVERRIDE_MIMALLOC`: Override entire memory allocation with mimalloc (default `OFF`).

### Additional Build Options for Distributed Indexes

- `DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_NUM`: The number of servers in a cluster (default `1`).
- `DBGROUP_TEST_DISTRIBUTED_INDEX_NODE_ID`: The ID of this server in a cluster (default `0`).

## Usage

...WIP (some sample files are in a `test` directory).

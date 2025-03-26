# Index Fixtures

This repository is a collection of fixtures for testing index implementations.

## Build Options

- `DBGROUP_INDEX_FIXTURES_DISABLE_<OPS>_TEST`: Disable unit tests for a specified operation (default `OFF`).
    - Target operations are `READ`, `SCAN`, `WRITE`, `INSERT`, `UPDATE`, `DELETE`, and `BULKLOAD`.
- `DBGROUP_TEST_THREAD_NUM`: The maximum number of threads to perform unit tests (default `2`).
- `DBGROUP_TEST_EXEC_NUM`: The number of executions per a thread (default `1E5`).
- `DBGROUP_TEST_RANDOM_SEED`: A fixed seed value to reproduce unit tests (default `0`).

## Usage

...WIP.

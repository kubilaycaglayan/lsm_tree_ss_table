# Key-Value Storage with Write-Ahead Log, LSM Tree, and SSTable

This repository is a practice project focused on building a key-value storage system using the SSTable architecture.

## Resources and Related Information

For a deeper understanding, check out these resources:
- [Understanding Log-Structured Merge Trees (LSM)](https://www.youtube.com/watch?v=ciGAVER_erw)
- [SSTables: The Building Blocks of LSM Trees](https://www.youtube.com/watch?v=6yJEwqseMY4)
- [Write-Ahead Logging Explained](https://www.youtube.com/watch?v=W_v05d_2RTo)

## How to Run

### Loading Dependencies

First, install the necessary dependencies:

```bash
bundle install
```

## Running the Program
You can run the program using the following command:

```bash
ruby bin/run.rb
```

## Running Unit Tests
To run the unit tests, use:

```bash
rspec --format documentation
```

## Basic representation of the class relationship

```mermaid

classDiagram
    class KeyValueStore {
        -MemTable @memtable
        -WAL @wal
        -SSTable @sstable
        +add(key, value)
        +get(key)
        +update(key, value)
        +delete(key)
        +flush_memtable()
        +memtable_size()
        +drop_store!()
    }

    class MemTable {
        -WAL @wal
        -AVLTree @data
        +add(key, value, timestamp)
        +get(key)
        +delete(key)
        +update(key, value, timestamp)
        +to_h()
        +size()
        +flush()
        +check_write_ahead_log_and_recover()
    }

    class SSTable {
        -String @dir
        -int @base_shard_id
        -List @shard_names
        -Map @files_memo
        -Map @file_lines_memo
        -int @level_depth
        +compute_shard_id()
        +write(data)
        +get(key, lvl)
        +compute_storage_state()
        +sort_shard_names()
        +last_shard_id(level)
        +read(file_path)
        +binary_search_in_file(file_path, key)
        +drop()
        +compress_if_needed(target_level)
        +merge_sort(data, data_recent)
    }

    class WAL {
        -String @file_path
        +write(key, value, timestamp, deleted)
        +read()
        +flush()
    }

    KeyValueStore --> MemTable
    KeyValueStore --> WAL
    KeyValueStore --> SSTable
    MemTable --> WAL
```

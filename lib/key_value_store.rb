require_relative 'memtable'
require_relative 'sstable'
require_relative 'wal'
require_relative 'exceptions'
require_relative '../config/constants'
require_relative '../config/prep'
require_relative '../config/paths'
require_relative '../utils/timehelper'

class KeyValueStore
  include Singleton, TimeHelper

  def initialize
    create_directories_if_not_exist
    @memtable = MemTable.instance
    @wal = WAL.instance
    @sstable = SSTable.instance
  end

  def add(key, value)
    timestamp = ts

    @wal.write(key.to_s, value, timestamp)
    @memtable.add(key.to_s, value, timestamp)

    flush_memtable if @memtable.size >= Constants::MEMTABLE_SIZE_LIMIT
  end

  def get(key)
    key_to_search = key.to_s

    result = @memtable.get(key_to_search)

    if result
      return result[:deleted] ? nil : result
    end

    result = @sstable.get(key_to_search)

    if result
      return result[:deleted] ? nil : result
    end

    nil
  end

  def update(key, value)
    timestamp = ts

    @wal.write(key, value, timestamp)
    @memtable.update(key, value, timestamp)
  end

  def delete(key)
    key_to_be_deleted = key.to_s

    @wal.write(key_to_be_deleted, nil, ts, true)

    if @memtable.get(key_to_be_deleted) || @sstable.get(key_to_be_deleted)
      @memtable.delete(key_to_be_deleted)
    else
      raise KeyNotFoundError.new(key_to_be_deleted)
    end
  end

  def flush_memtable
    @sstable.write(@memtable.to_h)

    @wal.flush
    @memtable.flush
  end

  def memtable_size
    @memtable.size
  end

  def drop_store!
    @memtable.flush
    @wal.flush
    @sstable.drop
  end
end

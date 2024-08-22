require_relative 'memtable'
require_relative 'sstable'
require_relative 'wal'
require_relative 'exceptions'
require_relative '../config/constants'
require_relative '../config/prep'
require_relative '../config/paths'

class KeyValueStore
  include Singleton

  def initialize
    create_directories_if_not_exist
    @memtable = MemTable.instance
    @wal = WAL.instance
    @sstable = SSTable.instance
  end

  def add(key, value)
    timestamp = Time.now.strftime('%Y%m%d%H%M%S')

    @wal.write(key, value, timestamp)
    @memtable.add(key, value, timestamp)

    flush_memtable if @memtable.size >= Constants::MEMTABLE_SIZE_LIMIT
  end

  def get(key)
    result = @memtable.get(key)

    if result
      return result[:deleted] ? nil : result
    end

    result = @sstable.get(key)

    if result
      return result[:deleted] ? nil : result
    end

    raise KeyNotFoundError.new(key)
  end

  def update(key, value)
    timestamp = Time.now.strftime('%Y%m%d%H%M%S')
    @wal.write(key, value, timestamp)
    @memtable.update(key, value, timestamp)
  end

  def delete(key)
    @wal.write(key, nil, Time.now.strftime('%Y%m%d%H%M%S'), true)

    if @memtable.get(key) || @sstable.get(key)
      @memtable.delete(key)
    else
      raise KeyNotFoundError.new(key)
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

  def drop_store
    @memtable.flush
    @wal.flush
    @sstable.drop
  end
end

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
    @wal.write({ key: key, value: value, timestamp: Time.now.strftime('%Y%m%d%H%M%S') })
    @memtable.add(key, value)

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
    @wal.write({ key: key, value: value, timestamp: Time.now.strftime('%Y%m%d%H%M%S') })
    @memtable.update(key, value)
  end

  def delete(key)
    @wal.write({ key: key, value: nil, deleted: true, timestamp: Time.now.strftime('%Y%m%d%H%M%S') })

    if @memtable.get(key) || @sstable.get(key)
      @memtable.delete(key)
    else
      raise KeyNotFoundError.new(key)
    end
  end

  def flush_memtable
    begin
      @sstable.write(@memtable.to_h)
    rescue StandardError => e
      puts e.message
      # TODO: Error handling
    else
      @wal.flush
      @memtable.flush
    end
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

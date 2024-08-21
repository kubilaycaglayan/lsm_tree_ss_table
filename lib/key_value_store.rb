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
    create_directories
    @memtable = MemTable.instance
    @wal = WAL.instance
  end

  def add(key, value)
    @wal.write({ key: key, value: value, timestamp: Time.now.strftime('%Y%m%d%H%M%S') })
    @memtable.add(key, value)

    flush_memtable if @memtable.size >= Constants::MEMTABLE_SIZE_LIMIT
  end

  def get(key)
    if @memtable.get(key)
      @memtable.get(key)[:deleted] ? nil : @memtable.get(key)
    elsif @sstable.get(key)
      @sstable.get(key)[:deleted] ? nil : @sstable.get(key)
    else
      raise KeyNotFoundError
    end
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
      raise KeyNotFoundError
    end
  end

  def flush_memtable
    begin
      timestamp = Time.now.strftime('%Y%m%d%H%M%S')
      sstable = SSTable.new("#{Paths::DATA_DIR_PATH}/sstable_#{timestamp}.dat")
      sstable.write(@memtable.to_h)
    rescue StandardError => e
      puts e.message
      # TODO: Error handling
    else
      @memtable.flush
      @wal.flush
    end
  end

  def memtable_size
    @memtable.size
  end

  def drop_store
    @memtable.flush
    @wal.flush
    # @sstable.drop
  end
end

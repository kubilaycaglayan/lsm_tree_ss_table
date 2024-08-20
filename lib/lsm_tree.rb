# TODO: Remove this file.

require 'avl_tree'
require_relative 'wal'
require_relative 'sstable'

class LSMTree
  def initialize(wal_path, sstable_path, memtable_limit = 30)
    # @memtable = AVLTree.new
    # @wal = WAL.new(wal_path)
    @sstable = SSTable.new(sstable_path)
    # @memtable_limit = memtable_limit
  end

=begin   def add(key, value)
    @memtable.insert(key, value)

    @write_count += 1

    flush_to_sstable if @write_count >= @memtable_limit
  end
=end
  # def get(key)
  #   @memtable.find(key) || read_from_sstable(key)
  # end

  def delete(key)
    # @memtable.delete(key)
    # @wal.write({ operation: 'delete', key: key })
    # flush_to_sstable if @write_count >= @memtable_limit
  end

  private

  # def flush_to_sstable
  #   data = @memtable.to_h
  #   @sstable.write(data)
  #   @memtable = AVLTree.new
  #   @write_count = 0
  # end

  def read_from_sstable(key)
    data = @sstable.read
    data[key]
  end
end

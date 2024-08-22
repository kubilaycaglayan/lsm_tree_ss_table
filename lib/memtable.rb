require 'avl_tree'
require 'singleton'

require_relative 'exceptions'
require_relative 'wal'
require_relative '../config/paths'
require_relative '../config/constants'

class MemTable
  include Singleton

  def initialize
    @wal = WAL.instance
    @data = AVLTree.new
    @memtable_size_limit = Constants::MEMTABLE_SIZE_LIMIT

    check_wal_and_load_data
  end

  def add(key, value)
    timestamp = Time.now.strftime('%Y%m%d%H%M%S')

    @data[key] = {
      value: value,
      timestamp: timestamp
    }
  end

  def get(key)
    @data[key]
  end

  def delete(key)
    raise KeyNotFoundError.new(key) unless @data[key]

    @data[key][:value] = nil
    @data[key][:deleted] = true
    @data[key][:timestamp] = Time.now.strftime('%Y%m%d%H%M%S')
  end

  def update(key, value)
    add(key, value)
  end

  def to_h
    @data
  end

  def size
    @data.size
  end

  def flush
    @data = AVLTree.new
  end

  def check_wal_and_load_data
    return unless File.exist?(Paths::WAL_FILE_PATH)

    File.open(Paths::WAL_FILE_PATH, 'r') do |file|
      file.each_line do |line|
        begin
          data = JSON.parse(line) unless line.nil?
        rescue JSON::ParserError
          puts "Error parsing JSON: \"#{line}\""
          next
        end
        @data[data['key']] = data['value']
      end
    end

    @wal.flush
  end
end

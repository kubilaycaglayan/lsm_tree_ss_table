require 'avl_tree'
require 'singleton'

require_relative 'exceptions'
require_relative 'wal'
require_relative '../config/paths'
require_relative '../config/constants'
require_relative '../utils/time_helper'

class MemTable
  include Singleton, TimeHelper

  def initialize
    @wal = WAL.instance
    @data = AVLTree.new

    check_write_ahead_log_and_recover
  end

  def add(key, value, timestamp)
    @data[key] = {
      value: value,
      timestamp: timestamp
    }
  end

  def get(key)
    @data[key]
  end

  def delete(key)
    @data[key] ||= {}

    @data[key][:value] = nil
    @data[key][:deleted] = true
    @data[key][:timestamp] = ts
  end

  def update(key, value, timestamp)
    add(key, value, timestamp)
  end

  def to_h
    @data.to_h
  end

  def size
    @data.size
  end

  def flush
    @data = AVLTree.new
  end

  def check_write_ahead_log_and_recover
    return unless File.exist?(Paths::WAL_FILE_PATH)

    File.open(Paths::WAL_FILE_PATH, 'r') do |file|
      file.each_line do |line|
        begin
          data = JSON.parse(line) unless line.nil?
        rescue JSON::ParserError
          puts "Error parsing JSON: \"#{line}\""
          next
        end
        @data[data['key']] = {
          value: data['value'],
          timestamp: data['timestamp']
        }
        @data[data['key']][:deleted] = true if data['deleted']
      end
    end
  end
end

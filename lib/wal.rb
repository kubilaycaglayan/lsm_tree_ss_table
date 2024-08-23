require 'json'
require 'singleton'

require_relative '../config/paths'

class WAL
  include Singleton

  def initialize
    File.new(Paths::WAL_FILE_PATH, 'w') unless File.exist?(Paths::WAL_FILE_PATH)
    @file_path = Paths::WAL_FILE_PATH
  end

  def write(key, value, timestamp, deleted = nil)
    data = { key:, value:, timestamp: }
    data[:deleted] = true if deleted

    File.open(@file_path, 'a') do |file|
      file.puts(data.to_json)
    end
  end

  def read
    data = []
    File.open(@file_path, 'r') do |file|
      file.each_line do |line|
        data << JSON.parse(line)
      end
    end
    data
  end

  def flush
    # puts "Flushing WAL #{@file_path}"
    File.open(@file_path, 'w') {}
  end
end
